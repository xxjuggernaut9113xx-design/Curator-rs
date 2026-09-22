# Curator packaging contracts

Each product consumes the workspace version (`0.3.3`) and keeps its library
ownership explicit:

- `curator` is **Curator Server**. It runs headlessly, serves the browser UI,
  and owns its SQLite data directory.
- `Curator` is **Curator Host**. It embeds the same backend in a native Slint shell.
- `curator-viewer` is **Curator Viewer**. It contains no database/server and
  only connects to Tailnet hosts.

Windows Server ships as separate current-user and all-users NSIS installers.
The current-user installer registers a scheduled task; the all-users installer
requires elevation and registers a Windows service. Both pass an explicit
installation scope to the executable. Their unchecked P-HAR checkbox records
`curator phar-intent --enabled true` before the first service/task launch, so
managed evaluation happens after Server startup and no installer transaction
downloads a model.

Linux produces an all-users `.deb` and a current-user portable archive. The
archive includes `install-current-user.sh` and a rendered systemd-user unit;
the `.deb` creates a `curator` service account and owns `/var/lib/curator`.

Host and Viewer Linux release jobs build both Debian packages and portable
AppImages. The app files may be placed per-user or system-wide; their settings
remain in each user's own application-data directory.

Host and Viewer app files may be installed per-user or system-wide, but their
preferences remain per-user. Do not package two owners of the same resolved
data directory: Curator's startup lock deliberately rejects that condition.

For a machine-wide Server migration, stop Host and run the elevated command:

```text
curator import-host --from "C:\path\to\host-data" --install-scope all-users
```

The command snapshots the Host database, copies library/media artifacts into
an empty Server directory, and refuses active or non-empty data directories.

Packaging and CI target Windows and Linux.
