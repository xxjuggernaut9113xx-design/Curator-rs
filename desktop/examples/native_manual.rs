//! Disposable Host window for native manual smoke checks.
//! Build with `cargo build --locked -p curator-desktop --example native_manual`.

fn seed_populated_library(state: &curator::AppState) -> Result<(), Box<dyn std::error::Error>> {
    let conn = state.pool.get()?;
    conn.execute_batch(
        "INSERT INTO sources(id,name,url,slug,status,added_at) VALUES
         (1,'Blue source','https://example.test/blue','blue','paused','2026-01-01'),
         (2,'Green source','https://example.test/green','green','paused','2026-01-01');
         INSERT INTO groups(id,name,added_at) VALUES(1,'Smoke group','2026-01-01');
         UPDATE sources SET group_id=1 WHERE id=2;",
    )?;
    let icon = include_bytes!("../icons/icon.png");
    for id in 1..=120 {
        let (source, slug) = if id <= 60 { (1, "blue") } else { (2, "green") };
        let filename = format!("sample-{id:03}.png");
        let relative = format!("{slug}/{filename}");
        let added_at = format!("2026-01-{:02}T{:02}:00:00", 1 + id / 24, id % 24);
        conn.execute(
            &format!(
                "INSERT INTO media(id,source_id,filepath,filename,type,rating,file_size_bytes,added_at)
                 VALUES({id},{source},'{relative}','{filename}','image',{}, {},'{added_at}')",
                id % 6,
                (id as u64) * 1024
            ),
            [],
        )?;
    }
    // Insert metadata before making the files visible to the background
    // scanner. Writing each file ahead of its row races the scan on Windows.
    for id in 1..=120 {
        let slug = if id <= 60 { "blue" } else { "green" };
        let path = state.library_dir.join(format!("{slug}/sample-{id:03}.png"));
        std::fs::create_dir_all(path.parent().ok_or("Invalid smoke media path")?)?;
        std::fs::write(path, icon)?;
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::temp_dir().join(format!(
        "curator-native-smoke-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    ));
    std::fs::create_dir_all(&root)?;
    // Set these before creating the runtime or starting any application work.
    // The example never reads or writes the installed Host configuration.
    std::env::set_var("CURATOR_CONFIG_DIR", root.join("Config"));
    std::env::set_var("CURATOR_DATA_DIR", root.join("Data"));
    std::env::set_var("CURATOR_NATIVE_PREFS_DIR", root.join("Preferences"));
    let result = (|| {
        let runtime = tokio::runtime::Runtime::new()?;
        let options = curator::edition::InitializeOptions {
            edition: curator::edition::Edition::Host,
            install_scope: curator::edition::InstallScope::CurrentUser,
            data_dir_override: Some(root.join("Data")),
        };
        let state = runtime.block_on(curator::initialize_with_options(options))?;
        if std::env::var_os("CURATOR_NATIVE_SMOKE_EMPTY").is_none()
            && !std::env::args().any(|argument| argument == "--empty")
        {
            seed_populated_library(&state)?;
        }
        let client = curator::native::LocalClient::new(state.clone())?;
        let result =
            curator_desktop::run_ui(&runtime, curator::native::Client::Local(client), false);
        runtime.block_on(curator::shutdown(&state));
        drop(state);
        drop(runtime);
        result
    })();
    let target = std::fs::canonicalize(&root)?;
    let temp = std::fs::canonicalize(std::env::temp_dir())?;
    if target.parent() != Some(temp.as_path())
        || !target
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("curator-native-smoke-"))
    {
        return Err("Refusing to remove an unexpected smoke directory".into());
    }
    let mut cleanup_error = None;
    for attempt in 0..20 {
        match std::fs::remove_dir_all(&target) {
            Ok(()) => {
                cleanup_error = None;
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                cleanup_error = None;
                break;
            }
            Err(error) => {
                cleanup_error = Some(error);
                if attempt < 19 {
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
            }
        }
    }
    if let Some(error) = cleanup_error {
        return Err(error.into());
    }
    result
}
