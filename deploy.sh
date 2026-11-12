#!/usr/bin/env bash
# Atualiza o site NO SERVIDOR: baixa a versão nova do GitHub, reconstrói, confere e,
# se algo falhar, volta para a versão que estava no ar.
# Uso:  cd /root/screenshare && ./deploy.sh
set -euo pipefail
cd "$(dirname "$0")"   # o nome desta pasta é o nome do projeto Compose: não mude

URL="https://edoganjore.com.br"
OLD="https://srv1597622.hstgr.cloud"   # host do VPS: planejador na raiz; /tela redireciona para $URL
say() { printf '\n-- %s\n' "$*"; }

grep -q '^TURN_PASS=.\+' .env 2>/dev/null || { echo "Falta TURN_PASS no arquivo .env (veja .env.example)." >&2; exit 1; }

PREV=$(git rev-parse HEAD)
say "baixando do GitHub"
git pull --ff-only
say "versão: $(git log -1 --format='%h %s')"

rollback() {
  say "FALHOU: $1"
  say "voltando para $(git log -1 --format='%h %s' "$PREV")"
  git reset -q --hard "$PREV"
  docker compose up -d --build || true
  exit 1
}

say "docker compose up -d --build"
docker compose up -d --build || rollback "build ou subida dos containers"

say "verificação"
ok=1
check() { if eval "$2"; then echo "  ok     $1"; else echo "  FALHA  $1"; ok=0; fi; }
for _ in $(seq 1 20); do
  docker compose logs --tail 20 screenshare 2>&1 | grep -q "screenshare ouvindo" && break; sleep 1
done
docker compose logs --tail 5 screenshare 2>&1 | sed 's/^/  | /'
check "log mostra 'screenshare ouvindo'" 'docker compose logs --tail 20 screenshare 2>&1 | grep -q "screenshare ouvindo em http://0.0.0.0:8080"'
sleep 3   # o Traefik leva um instante para ver o container novo
check "$URL/ responde 200 com a página" '[ "$(curl -s -o /tmp/tela.html -w "%{http_code}" "$URL/")" = 200 ] && grep -q "<title>Janjan" /tmp/tela.html'
check "$URL/config devolve iceServers" 'curl -s "$URL/config" | grep -q iceServers'
check "$URL/rooms responde" 'curl -s "$URL/rooms" | grep -q "\"rooms\""'
check "$OLD/tela/ redireciona para $URL" 'curl -sI "$OLD/tela/?room=x" | tr -d "\r" | grep -qix "location: $URL/?room=x"'
check "$OLD/ continua com o planejador" 'code=$(curl -s -o /tmp/root.out -w "%{http_code}" "$OLD/"); [ "$code" != 000 ] && [ "$code" -lt 500 ] && ! grep -q "<title>Janjan" /tmp/root.out'
check "coturn rodando" 'docker compose ps --status running --services | grep -q coturn'
echo "  login com Discord: $(curl -s "$URL/me" | grep -q '"login":true' && echo ligado || echo 'desligado (preencha o .env)')"

[ "$ok" = 1 ] || rollback "verificação"
say "no ar: $URL/"
