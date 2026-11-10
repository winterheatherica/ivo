# IVO documentation

| Document | Contents |
|---|---|
| [system-design.md](system-design.md) | System design version 3: concept, architecture, data flows, infrastructure, and plan |
| [adr/](adr/README.md) | Architecture Decision Records: the reasoning behind every important decision |
| [detail/](detail/README.md) | Detailed design: the flow from posting to comments, state machines, the API gateway, and the Postgres and MongoDB schemas |
| [../proto/](../proto/README.md) | Protobuf contracts for gRPC and events |

Diagrams are written in Mermaid, so GitHub renders them directly without separate image files.

## Version history

- **Version 3** (9 November 2026): resolves six conflicts in version 2 and adds the time model, privacy and account deletion, and capacity and cost. The summary is in [section 2](system-design.md#2-changes-from-version-2).
- **Version 2** (9 November 2026): the influencer-manager game concept, which replaced Hearthline. It was written in Indonesian as `Idol_Venturing_Out_Desain_Sistem_v2.pdf`, which is kept outside this repository.
- **Hearthline** (26 October 2026): a companion chat concept that is no longer used.
