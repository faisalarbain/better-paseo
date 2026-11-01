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
  });
});
