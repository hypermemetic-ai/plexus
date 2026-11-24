//! Set PLEXUS_MCP_TOKEN, then send Authorization: Bearer <token> to
//! http://127.0.0.1:3000/mcp.
use kiln::Kiln;
use plexus_layers_mcp::Mcp;

fn main() -> std::io::Result<()> {
    plexus::main([Kiln::default()]).mcp().run()
}
