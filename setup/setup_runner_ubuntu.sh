#!/usr/bin/env bash
set -euo pipefail

POSTGRES_VERSION="${POSTGRES_VERSION:-17}"
REPOSITORY_URL="${REPOSITORY_URL:-https://github.com/hqakhtar/pg_benchmark.git}"
REPOSITORY_DESTINATION="${REPOSITORY_DESTINATION:-$HOME/pg_benchmark}"

sudo apt-get update
sudo env DEBIAN_FRONTEND=noninteractive apt-get upgrade -y
sudo apt-get install -y curl ca-certificates git gnupg lsb-release rsync screen
sudo install -d /usr/share/postgresql-common/pgdg
curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
	| sudo gpg --dearmor --yes -o /usr/share/postgresql-common/pgdg/pgdg.gpg
echo "deb [signed-by=/usr/share/postgresql-common/pgdg/pgdg.gpg] \
http://apt.postgresql.org/pub/repos/apt \
$(lsb_release -cs)-pgdg main" | sudo tee /etc/apt/sources.list.d/pgdg.list
sudo apt-get update
sudo apt-get install -y \
	"postgresql-$POSTGRES_VERSION" \
	"postgresql-client-$POSTGRES_VERSION" \
	"postgresql-server-dev-$POSTGRES_VERSION"
sudo systemctl disable --now postgresql
command -v psql

cat > ~/.screenrc <<EOL
defscrollback 100000        # Buffer size for scrollback history
scrollback 100000           # Increase scrollback buffer size per window
termcapinfo xterm* ti@:te@  # Mouse scroll
bindkey -m ^[[5~ stuff ^b   # PgUp in copy mode
bindkey -m ^[[6~ stuff ^f   # PgDn in copy mode
EOL

grep -qxF "ulimit -n 65536" ~/.bashrc || echo "ulimit -n 65536" >> ~/.bashrc

if [[ -d "$REPOSITORY_DESTINATION/.git" ]]; then
	git -C "$REPOSITORY_DESTINATION" pull --ff-only
elif [[ -e "$REPOSITORY_DESTINATION" ]]; then
	echo "Repository destination exists but is not a Git checkout:" \
		"$REPOSITORY_DESTINATION" >&2
	exit 1
else
	git clone "$REPOSITORY_URL" "$REPOSITORY_DESTINATION"
fi

