#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Runtime::new()?;
    let state = runtime.block_on(curator::initialize_host())?;
    let arguments: Vec<String> = std::env::args().collect();
    let background = arguments.iter().any(|argument| argument == "--background");
    // A background launch keeps the host alive in the tray; remote access is
    // part of that job, so it starts the HTTP service too.
    let serve = background || arguments.iter().any(|argument| argument == "--serve");
    // An unavailable optional listener must never prevent local use.
    if serve {
        if let Err(error) = runtime.block_on(curator::remote::start_http_server(&state)) {
            eprintln!("Remote access could not start; local library remains available: {error}");
        }
    }
    let client = curator::native::LocalClient::new(state.clone())?;
    let result =
        curator_desktop::run_ui(&runtime, curator::native::Client::Local(client), background);
    runtime.block_on(curator::shutdown(&state));
    result
}
