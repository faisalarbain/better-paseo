import { describe, expect, it, vi } from "vitest";
import { createCompositePaseo } from "../../client/tasks/composite-paseo";

function api(serverId: string, agentId: string) {
  const refresh = vi.fn(async () => ({ agent: { id: agentId }, project: null }));
  const ref = vi.fn(() => ({ refresh }));
  const paseo = {
    hosts: {
      list: async () => ({
        self: { serverId: "srv_local", label: "Local" },
        hosts: [{ serverId: "srv_remote", label: "Remote", enabled: true }],
      }),
      client: async () => remote,
    },
    agents: {
      list: async () => ({ requestId: "req", entries: [{ agent: { id: agentId } }], pageInfo: { nextCursor: null } }),
      ref,
    },
    workspaces: {
      list: async () => ({ requestId: "req-ws", entries: [{ id: `${serverId}-workspace` }], pageInfo: { nextCursor: null } }),
      ref: vi.fn((id: string) => ({ refresh: async () => ({ id }) })),
    },
    dispose: async () => {},
  } as any;
  return { paseo, ref, refresh };
}

const remote = {} as any;

describe("createCompositePaseo", () => {
  it("keeps host identity on merged agents and routes ref to the owning API", async () => {
    const local = api("srv_local", "local-agent");
    const remoteApi = api("srv_remote", "remote-agent");
    Object.assign(remote, remoteApi.paseo);
    const composite = await createCompositePaseo(local.paseo);
    const result = await composite.agents.list({});
    const remoteEntry = result.entries.find((entry: any) => entry.agent.id === "remote-agent");

    expect(remoteEntry?.agent).toMatchObject({ _hostIndex: 1, _hostServerId: "srv_remote", _hostLabel: "Remote" });
    await composite.agents.ref("remote-agent").refresh();
    expect(remoteApi.ref).toHaveBeenCalledWith("remote-agent");
    expect(local.ref).not.toHaveBeenCalledWith("remote-agent");
    const workspaces = await composite.workspaces.list({ subscribe: {} } as any);
    expect(workspaces.entries.map((workspace: any) => workspace.id)).toEqual(["srv_local-workspace", "srv_remote-workspace"]);
    await composite.workspaces.ref("srv_remote-workspace").refresh();
    expect(remoteApi.paseo.workspaces.ref).toHaveBeenCalledWith("srv_remote-workspace");
  });
});
