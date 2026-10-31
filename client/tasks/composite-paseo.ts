import type { PaseoApi } from "@getpaseo/client";

/**
 * A PaseoApi that fans agent reads out to several hosts and merges the
 * results, so the Tasks board can show agents from every registered host in a
 * single list. Workspaces, projects and providers are delegated to the primary
 * (self) host, which Paseo already keeps unified across hosts.
 */
export async function createCompositePaseo(self: PaseoApi): Promise<PaseoApi> {
  let hosts: Array<{ serverId: string | null; label: string | null; enabled: boolean }> = [];
  try {
    const dir = await self.hosts.list();
    hosts = dir.hosts ?? [];
  } catch {
    hosts = [];
  }

  const apis: PaseoApi[] = [self];
  for (const h of hosts) {
    if (!h.enabled || !h.serverId) continue;
    try {
      const api = await self.hosts.client(h.serverId);
      apis.push(api);
    } catch {
      // Offline host: skip.
    }
  }

  const compositeAgents = {
    list: async (options: any) => {
      const results = await Promise.all(apis.map((api) => api.agents.list(options).catch(() => null)));
      const entries = results.flatMap((r) => (r ? r.entries : []));
      const compositeSub = createCompositeSubscription(
        results.map((r, i) => (r && r.subscription ? { sub: r.subscription, idx: i } : null)).filter(Boolean) as any,
      );
      const pageInfo = results.find((r) => r?.pageInfo)?.pageInfo ?? { nextCursor: null };
      return { entries, subscription: compositeSub, pageInfo };
    },
    ref: (id: string) => self.agents.ref(id),
  };

  const composite = new Proxy(self, {
    get(target, prop, receiver) {
      if (prop === "agents") return compositeAgents;
      if (prop === "dispose") {
        return async () => { await Promise.all(apis.map((a) => a.dispose().catch(() => {}))); };
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
