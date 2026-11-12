# Janjan's Path Edo Ganjore

Salas de compartilhamento de tela pelo navegador, com um servidor em Rust (axum).
No ar em **https://edoganjore.com.br**.

**Como funciona:** o servidor serve a página, cuida de quem está em cada sala e faz a
sinalização WebRTC por WebSocket. O vídeo vai **direto** de quem compartilha para cada
espectador (P2P), ou via TURN quando o NAT bloqueia. O servidor nunca toca no vídeo, por
isso usa poucos MB de RAM.

## Funcionalidades

- **Um clique**: "Compartilhar tela", escolhe a janela e já está na sala com o convite copiado.
- **Sala com várias telas**: várias pessoas compartilham ao mesmo tempo (até `MAX_HOSTS`),
  em grade ou com uma em destaque. Cada tela tem volume próprio, tela cheia, zoom
  (Ctrl + roda do mouse) e janela flutuante (fica por cima do jogo).
- **Pessoas na sala**: lista de quem está presente, quem está ao vivo, quem é o dono e quem
  entrou como convidado. O olho ao lado de cada pessoa oculta a tela dela só para você (o
  vídeo deixa de ser enviado para o seu PC); "Ocultar minha tela" esconde só a sua prévia.
- **Entrar com Discord ou só com nick**: com Discord, o nick e o avatar vêm da conta; sem,
  a pessoa digita um nick e aparece como convidada.
- **Quem pode entrar** (definido por quem cria a sala): qualquer um com o link, só quem
  entrou com Discord, com senha, ou com aprovação do dono. Se o dono sai, a pessoa há mais
  tempo na sala passa a responder os pedidos.
- **Ao vivo agora**: a tela inicial lista as salas visíveis em que alguém está compartilhando.
  Quem cria a sala pode desmarcar essa opção.
- **Qualidade**: padrão de jogos, 60 fps em H.264 (o codec que os navegadores codificam pela
  placa de vídeo). Se o PC de quem compartilha ficar no limite por ~20 s, cai sozinho para
  30 fps. Dá para trocar perfil e codec ao vivo.
- **Diagnóstico**: o botão de qualidade mostra codec, encoder (placa de vídeo ou processador),
  o que está limitando (PC ou internet), fps capturado × enviado, taxa, e se a conexão é
  direta ou via servidor (TURN).
- **Atalhos**: `F` tela cheia, `G` grade/destaque, `P` pausar.

## Rodar localmente

```bash
cargo run --release
# abra http://localhost:8080  (localhost conta como contexto seguro)
```

Sem as variáveis do Discord, todo mundo entra só com nick.

## Deploy (VPS Hostinger com Traefik do Docker Manager)

O `docker-compose.yml` usa o Traefik que já roda no VPS (em `network_mode: host`, entrypoint
`websecure`, certresolver `letsencrypt`); só as labels bastam, e o HTTPS sai automático.
`www.edoganjore.com.br` e o endereço antigo (`srv1597622.hstgr.cloud/tela`) redirecionam
para o domínio.

Primeira vez no servidor:

1. DNS: registros **A** para `@` e `www` apontando para o IP do VPS.
2. Firewall: `3478/tcp+udp` e `49160-49200/udp` (TURN).
3. Clone em `/root/screenshare` (o nome da pasta é o nome do projeto Compose).
4. Crie o `.env` a partir do `.env.example` (senha do TURN e, se quiser, Discord).
5. `docker compose up -d --build`

Atualizações: no servidor, `cd /root/screenshare && ./deploy.sh`. Ele faz `git pull`,
reconstrói, confere o site e volta para a versão anterior se algo falhar.

## Login com Discord

1. Em <https://discord.com/developers/applications>, crie uma aplicação.
2. Em **OAuth2**, copie o **Client ID** e o **Client Secret**, e adicione o redirect
   `https://edoganjore.com.br/auth/callback` (tem que ser idêntico a `PUBLIC_URL` + `/auth/callback`).
3. Preencha `DISCORD_CLIENT_ID` e `DISCORD_CLIENT_SECRET` no `.env` e rode `docker compose up -d`.
4. Opcional: `DISCORD_GUILD_ID` com o ID do seu servidor do Discord, para só membros dele
   conseguirem entrar com Discord.

As sessões ficam na memória do servidor e valem 30 dias; reiniciar o container pede novo login.

## Discord: aviso de sala ao vivo e comando `/tela`

Os dois são opcionais e independentes.

**Aviso no canal.** Quando alguém começa a compartilhar numa sala visível, o servidor posta
no canal "Fulano está ao vivo" com o link (no máximo um aviso por sala a cada 5 minutos).

1. No Discord: Editar canal -> Integrações -> Webhooks -> Novo webhook -> Copiar URL do webhook.
2. Coloque a URL em `DISCORD_WEBHOOK_URL` no `.env` e rode `docker compose up -d`.

**Comando `/tela`.** Responde no canal com o botão "Entrar na sala". Sem argumento, a sala
tem o nome do canal; `/tela sala:nome` escolhe outro. Usa o mesmo app do login.

1. No portal de desenvolvedores, em General Information, copie a **Public Key** para
   `DISCORD_PUBLIC_KEY` no `.env` e rode `docker compose up -d`. O log deve mostrar
   "comando /tela registrado no Discord".
2. Na mesma página, preencha **Interactions Endpoint URL** com
   `https://edoganjore.com.br/discord/interactions` e salve (o Discord testa o endereço na hora).
3. Instale o app no seu servidor do Discord abrindo
   `https://discord.com/oauth2/authorize?client_id=SEU_CLIENT_ID&scope=applications.commands`.

## Variáveis de ambiente

| Variável      | Padrão                           | Uso                                             |
|---------------|----------------------------------|-------------------------------------------------|
| `BIND`        | `0.0.0.0:8080`                   | Endereço do servidor                            |
| `MAX_VIEWERS` | `10`                             | Pessoas por sala                                |
| `MAX_HOSTS`   | `6`                              | Telas compartilhadas ao mesmo tempo por sala    |
| `STUN_URL`    | `stun:stun.l.google.com:19302`   | STUN                                            |
| `TURN_URL`    | —                                | Um ou mais URLs separados por vírgula           |
| `TURN_USER` / `TURN_PASS` | —                    | Credenciais do TURN                             |
| `PUBLIC_URL`  | —                                | Endereço público da página (obrigatório com Discord) |
| `DISCORD_CLIENT_ID` / `DISCORD_CLIENT_SECRET` | — | Liga o login com Discord                      |
| `DISCORD_GUILD_ID` | —                           | Restringe o login aos membros de um servidor    |
| `DISCORD_WEBHOOK_URL` | —                        | Canal que recebe o aviso de sala ao vivo        |
| `DISCORD_PUBLIC_KEY` | —                         | Liga o comando `/tela`                          |

## Registro

O servidor anota quem pediu para entrar, entrou e saiu de cada sala, e por quanto tempo:

```bash
docker compose logs -t screenshare | tail -30
```

O vídeo não passa pelo servidor, então não há log dele; para isso use o Diagnóstico da página.

## Limite do modelo P2P

Cada pessoa que compartilha codifica **um stream por espectador**, e cada espectador baixa
um stream por tela. No perfil de jogos são até 10 Mb/s de upload por espectador. Funciona
bem até umas 5 a 10 pessoas por sala, dependendo do upload e do PC de quem compartilha.
Para dezenas de espectadores, o passo seguinte é um SFU (quem compartilha envia uma vez e o
servidor só repassa pacotes RTP). Em Rust, a base natural para isso é o crate `str0m`.
