# Run NebulaFS with Docker Compose

Use Docker Desktop in Linux container mode on Windows. From the repository root, build and start the single-node server:

```sh
docker compose -f deploy/compose/docker-compose.yml up --build --detach --wait
docker compose -f deploy/compose/docker-compose.yml ps
```

Compose builds a Linux release image from source. It publishes port 8080 on loopback only and checks both `/healthz` and `/readyz`. To check from Windows, use `curl.exe http://127.0.0.1:8080/readyz` in PowerShell. Set `NEBULAFS_HOST_PORT` if that host port is already in use.

The default server configuration comes from `config/server.json` and enables JWT authentication. For deployment-specific settings, place a server JSON file outside the repository and set `NEBULAFS_CONFIG_FILE` to its absolute path before running Compose. The container mounts it read-only. Keep the container listener on port 8080 with TLS terminated by the local tunnel. Keep the object, temporary, and SQLite paths under `/app/data`.

Example in PowerShell:

```powershell
$env:NEBULAFS_CONFIG_FILE = 'C:/NebulaFS/server.json'
docker compose -f deploy/compose/docker-compose.yml up --build --detach --wait
```

## Start the pilot

Generate two independent 64-character hexadecimal values using a secure random generator. Keep them in a private env file outside the checkout, such as `C:/NebulaFS/pilot.env`, with these variable names:

- `NEBULAFS_PILOT_UPLOAD_TOKEN` is the invite credential given to senders.
- `NEBULAFS_PILOT_DOWNLOAD_KEY` signs download links and stays on the host. Changing it invalidates existing links.

Run from the repository root in PowerShell:

```powershell
docker compose --env-file C:/NebulaFS/pilot.env -p nebulafs-pilot -f deploy/compose/docker-compose.yml -f deploy/compose/docker-compose.pilot.yml up --build --detach --wait
```

Use `-p nebulafs-pilot` and both configuration files on later invocations so they target the same project and data volume. Set `NEBULAFS_PILOT_CONFIG_FILE` to an absolute host path if you need to override the pilot server JSON. Verify invite enforcement and link access locally before pointing the named tunnel at `http://127.0.0.1:8080`. Keep the tunnel credential outside Git.

## Windows startup and recovery

The Windows laptop owner operates the pilot. Keep the laptop plugged in and set plugged-in sleep and lid behavior so the machine remains awake. Enable **Start Docker Desktop when you sign in to your computer**. Compose restarts the server once the engine is running. Recovery before Windows sign-in is a separate capability and must be verified if needed. [Docker Desktop settings](https://docs.docker.com/desktop/settings-and-maintenance/settings/)

In Cloudflare, create a remotely managed named tunnel and install its Windows connector as a service using the dashboard command. Publish `send.nebulafs.app` to `http://127.0.0.1:8080` or the configured loopback port. Reboot the laptop, sign in if needed, then check Compose health, the tunnel status, the final browser URL, and recovery of an upload with at least one acknowledged part. Record the tested commit and whether sign-in was required. [Cloudflare tunnel setup](https://developers.cloudflare.com/tunnel/setup/)

If the endpoint stays unavailable, check Docker Desktop and the Compose service locally first, then check the Windows tunnel service and hostname. Reuse the existing Compose project and data volume when restarting. Continue with the [route smoke test](../../docs/runbooks/pilot-benchmark.md) once the endpoint works.

## Persistent files

The named volume `nebulafs-data` holds uploaded files, staged parts, and SQLite metadata. For a 12 GB run, check that the Windows drive hosting Docker’s disk image and the Docker data volume both have at least 34 GB available after accounting for retained files. Rebuilding the image, restarting the service, and replacing the container keep that volume:

```sh
docker compose -f deploy/compose/docker-compose.yml up --detach --force-recreate --wait
```

Keep the Compose project name stable across deployments. Avoid `down --volumes` for a live installation because it removes stored data. For laptop pickup, open the bearer link’s path on `http://127.0.0.1:8080` to download directly from the local origin. The host owner can also use `docker compose cp` to copy a known finalized object to the Windows filesystem.

## Local packaging check

For a throwaway smoke run, use an alternate Compose project, unused host port, and a copy of the server config with auth disabled. Keep the service on loopback and leave it disconnected from any public tunnel. Check `/healthz` and `/readyz`, create a bucket, upload a small object, and download it. Recreate the container and download the same object again. Compare bytes with the original to verify object and SQLite persistence together.
