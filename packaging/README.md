# Running punctual as a service

`punctual` is not its own supervisor (DESIGN D2) — it expects the OS to keep it
alive and to restart it on crash. Both units below send a graceful stop signal
that `punctual` treats as *drain*: stop claiming new fires, let in-flight jobs
finish, then exit.

Once it's running, talk to it over its control socket:

```console
punctual ping            # is it alive, how many jobs, how many in flight
punctual reload          # apply added / removed jobs (changed jobs need a restart)
punctual stop --kill     # drain, or hard-kill in-flight jobs and exit now
```

The systemd unit wires `systemctl --user reload punctual` to `punctual reload`.

## Linux — systemd (user service, no root)

```console
mkdir -p ~/.config/systemd/user
cp systemd/punctual.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now punctual
journalctl --user -u punctual -f
```

`loginctl enable-linger $USER` if you want it running while you're logged out.
Edit the unit for a system-wide install (move to `/etc/systemd/system/`, set
`User=`, use absolute paths).

## macOS — launchd (LaunchAgent)

```console
sed "s|USERNAME|$USER|g" launchd/com.github.thejohnmatti.punctual.plist \
  > ~/Library/LaunchAgents/com.github.thejohnmatti.punctual.plist
launchctl load -w ~/Library/LaunchAgents/com.github.thejohnmatti.punctual.plist
tail -f ~/Library/Logs/punctual.log
```

Unload with `launchctl unload -w ~/Library/LaunchAgents/com.github.thejohnmatti.punctual.plist`.

## Docker

```console
docker run --restart=always -v "$PWD/punctual.toml:/etc/punctual/punctual.toml:ro" \
  -v punctual-state:/state -e PUNCTUAL_DB=/state/punctual.db \
  ghcr.io/thejohnmatti/punctual run -c /etc/punctual/punctual.toml
```

*(image not published yet — build from the repo for now)*
