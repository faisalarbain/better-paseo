import type { PaseoApi } from "@getpaseo/client";

/**
 * A PaseoApi that fans agent reads out to several hosts and merges the
 * results, so the Tasks board can show agents from every registered host in a
 * single list. Workspaces, projects and providers are delegated to the primary
 * (self) host, which Paseo already keeps unified across hosts.
 */
export async function createCompositePaseo(self: PaseoApi): Promise<PaseoApi> {
  let hosts: Array<{ serverId: string | null; label: string | null; enabled: boolean }> = [];
  let selfServerId = "self";
  let selfLabel = "This host";
  try {
    const dir = await self.hosts.list();
    hosts = dir.hosts ?? [];
    selfServerId = dir.self.serverId;
    selfLabel = dir.self.label;
  } catch {
    hosts = [];
  }

  const hostApis: Array<{ api: PaseoApi; serverId: string; label: string }> = [
    { api: self, serverId: selfServerId, label: selfLabel },
  ];
  const agentHosts = new Map<string, PaseoApi>();
  const hostErrors: string[] = [];
  for (const h of hosts) {
    if (!h.enabled || !h.serverId) continue;
    try {
      const api = await self.hosts.client(h.serverId);
      hostApis.push({ api, serverId: h.serverId, label: h.label ?? h.serverId });
    } catch (e) {
      hostErrors.push(`${h.serverId}: ${e instanceof Error ? e.message : String(e)}`);
    }
  }
  // Expose debug info globally for troubleshooting.
  (globalThis as any).__compositeDebug = { hostCount: hostApis.length, apisCount: hostApis.length, hostErrors };
  const workspaceHosts = new Map<string, PaseoApi>();

  const compositeWorkspaces = new Proxy(self.workspaces, {
    get(target, prop, receiver) {
      if (prop === "list") {
        return async (options: any = {}) => {
          // The Tasks store consumes initial lists and explicitly refreshes;
          // don't create per-host subscriptions that it cannot release/merge.
          const { subscribe: _subscribe, ...query } = options;
          const results = await Promise.all(hostApis.map(({ api }) => api.workspaces.list(query).catch(() => null)));
          const entries = results.flatMap((result, index) => {
            if (!result) return [];
            for (const workspace of result.entries) workspaceHosts.set(workspace.id, hostApis[index]!.api);
            return result.entries;
          });
          return {
            requestId: results.find(Boolean)?.requestId ?? "composite-workspaces",
            entries,
            pageInfo: { nextCursor: null },
          };
        };
      }
      if (prop === "ref") {
        return (id: string) => (workspaceHosts.get(id) ?? self).workspaces.ref(id);
      }
      return Reflect.get(target, prop, receiver);
    },
  });

  const compositeAgents = {
    list: async (options: any) => {
      const { subscribe: _subscribe, ...query } = options ?? {};
      const results = await Promise.all(hostApis.map(({ api }) => api.agents.list(query).catch(() => null)));
      const entries = results.flatMap((r, i) => (r ? r.entries.map((entry: any) => {
        agentHosts.set(entry.agent.id, hostApis[i]!.api);
        return {
          ...entry,
          // Keep provenance on the agent itself: the Tasks store unwraps
          // `entry.agent` before building cards and classifying its workspace.
          agent: {
            ...entry.agent,
            _hostIndex: i,
            _hostServerId: hostApis[i]!.serverId,
            _hostLabel: hostApis[i]!.label,
          },
        };
      }) : []));
      // No subscription: the board uses the merged `entries` from results.flatMap,
      // so all hosts' agents show. Live updates for remote hosts are not wired
      // yet; the board refreshes on reopen.
      const compositeSub = null;
      const pageInfo = results.find((r) => r?.pageInfo)?.pageInfo ?? { nextCursor: null };
      return { entries, subscription: compositeSub, pageInfo };
    },
    ref: (id: string) => (agentHosts.get(id) ?? self).agents.ref(id),
  };

  const composite = new Proxy(self, {
    get(target, prop, receiver) {
      if (prop === "agents") return compositeAgents;
      if (prop === "workspaces") return compositeWorkspaces;
      if (prop === "dispose") {
        return async () => { await Promise.all(hostApis.map(({ api }) => api.dispose().catch(() => {}))); };
      }
      return Reflect.get(target, prop, receiver);
    },
  }) as PaseoApi;

  return composite;
}

function createCompositeSubscription(subs: Array<{ sub: any; idx: number }>) {
  const updateCallbacks: Array<(m: any) => void> = [];
  const errorCallbacks: Array<(e: any) => void> = [];
  const snapshotCallbacks: Array<(snap: any) => void> = [];

  let pending = subs.length;
  const mergedEntries = new Map<string, any>();
  const readyResolvers: Array<(v: any) => void> = [];
  let readyPromise: Promise<any> = new Promise((resolve) => { readyResolvers.push(resolve); });

  // Subscribe to each host up front.
  const releases: Array<() => void> = [];
  for (const { sub } of subs) {
    const unsub = sub.subscribe({
      snapshot: (snap: any) => {
        for (const e of snap?.entries ?? []) mergedEntries.set(e.agent.id, e);
        pending -= 1;
        if (pending <= 0) {
          const merged = { entries: [...mergedEntries.values()], pageInfo: { nextCursor: null } };
          for (const cb of [...snapshotCallbacks]) cb(merged);
          for (const r of readyResolvers) r(merged);
          readyResolvers.length = 0;
        }
      },
      update: (msg: any) => {
        for (const cb of [...updateCallbacks]) cb(msg);
      },
      error: (err: any) => {
        for (const cb of [...errorCallbacks]) cb(err);
      },
    });
    releases.push(() => { try { sub.release(); } catch {} if (typeof unsub === "function") unsub(); });
  }
  // If no subs, resolve immediately.
  if (subs.length === 0) {
    const merged = { entries: [] };
    queueMicrotask(() => { for (const r of readyResolvers) r(merged); readyResolvers.length = 0; });
  }

  return {
    get ready() { return readyPromise; },
    subscribe(observer: any) {
      snapshotCallbacks.push((snap: any) => observer.snapshot?.(snap));
      updateCallbacks.push((m: any) => observer.update?.(m));
      if (observer.error) errorCallbacks.push((e: any) => observer.error?.(e));
      // If already resolved, fire snapshot now.
      if (pending <= 0) {
        queueMicrotask(() => observer.snapshot?.({ entries: [...mergedEntries.values()], pageInfo: { nextCursor: null } }));
      }
      return () => {
        const i = snapshotCallbacks.indexOf(observer.snapshot);
        if (i >= 0) snapshotCallbacks.splice(i, 1);
      };
    },
    release: async () => { for (const r of releases) r(); },
  };
}
