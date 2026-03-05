sudo apt update -y
sudo apt upgrade -y
sudo apt install -y curl ca-certificates gnupg lsb-release
sudo install -d /usr/share/postgresql-common/pgdg
curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc   | sudo gpg --dearmor -o /usr/share/postgresql-common/pgdg/pgdg.gpg
echo "deb [signed-by=/usr/share/postgresql-common/pgdg/pgdg.gpg] \
http://apt.postgresql.org/pub/repos/apt \
$(lsb_release -cs)-pgdg main" | sudo tee /etc/apt/sources.list.d/pgdg.list
sudo apt update -y
sudo apt upgrade -y
sudo apt install -y postgresql-17 postgresql-client-17
sudo apt install -y postgresql-server-dev-17
sudo systemctl stop postgresql
sudo systemctl disable postgresql
systemctl status postgresql
which psql

sudo apt install -y screen
cat > ~/.screenrc <<EOL
defscrollback 100000        # Buffer size for scrollback history
scrollback 100000           # Increase scrollback buffer size per window
termcapinfo xterm* ti@:te@  # Mouse scroll
bindkey -m ^[[5~ stuff ^b   # PgUp in copy mode
bindkey -m ^[[6~ stuff ^f   # PgDn in copy mode
EOL

echo "ulimit -n 65536" >> ~/.bashrc
