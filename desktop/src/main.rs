#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Runtime::new()?;
    let state = runtime.block_on(curator::initialize_host())?;
    // An unavailable optional listener must never prevent local use.
    if std::env::args().any(|argument| argument == "--serve") {
        if let Err(error) = runtime.block_on(curator::remote::start_http_server(&state)) {
            eprintln!("Remote access could not start; local library remains available: {error}");
        }
    }
    let client = curator::native::LocalClient::new(state.clone())?;
    let result = curator_desktop::run_ui(&runtime, curator::native::Client::Local(client));
    runtime.block_on(curator::shutdown(&state));
    result
}
