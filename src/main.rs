//! Servidor de compartilhamento de tela.
//!
//! O vídeo NÃO passa por aqui: o servidor só serve a página, cuida de quem está em cada
//! sala (dono, pessoas, pedidos de entrada) e faz a sinalização WebRTC (troca de SDP/ICE)
//! por WebSocket. A mídia vai direto de quem compartilha para cada espectador (P2P), ou via
//! TURN quando o NAT bloquear. Resultado: poucos MB de RAM no servidor.

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Query, State,
    },
    http::{
        header::{CACHE_CONTROL, CONTENT_TYPE, COOKIE, SET_COOKIE},
        HeaderMap, StatusCode,
    },
    response::{Html, IntoResponse, Redirect, Response},
    routing::get,
    Json, Router,
};
use futures_util::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, oneshot};

const INDEX: &str = include_str!("../static/index.html");
const FAVICON: &[u8] = include_bytes!("../static/favicon.png");

#[derive(Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Role {
    Host,
    Viewer,
}

/// Quem pode entrar numa sala. Quem escolhe é quem cria a sala.
#[derive(Clone, Copy, PartialEq, Eq, Default)]
enum Access {
    #[default]
    Open,
    /// Só quem entrou com Discord (convidado com nick não entra).
    Discord,
    /// O dono da sala aprova cada pessoa.
    Approve,
    Password,
}

impl Access {
    fn parse(s: &str) -> Self {
        match s {
            "discord" => Self::Discord,
            "approve" => Self::Approve,
            "password" => Self::Password,
            _ => Self::Open,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Discord => "discord",
            Self::Approve => "approve",
            Self::Password => "password",
        }
    }
}

#[derive(Deserialize)]
struct JoinParams {
    room: String,
    role: Role,
    /// Nick de convidado. Com sessão do Discord, o servidor troca pelo nick da conta.
    #[serde(default)]
    name: String,
    /// Senha digitada por quem quer entrar numa sala com senha.
    #[serde(default)]
    key: String,
    /// Identificador aleatório da aba. Cada pessoa usa uma conexão para estar na sala e
    /// outra para compartilhar a tela, as duas com o mesmo valor.
    #[serde(default)]
    client: String,
    // ---- configurações de quem CRIA a sala (ignoradas se ela já existe) ----
    #[serde(default)]
    access: String,
    #[serde(default)]
    pass: String,
    /// "1" = não aparecer na lista "Ao vivo agora".
    #[serde(default)]
    unlisted: String,
    // ---- só o servidor preenche, a partir da sessão ----
    #[serde(skip)]
    avatar: String,
    /// Entrou só com nick, sem conta do Discord.
    #[serde(skip)]
    guest: bool,
}

/// Usuário autenticado pelo Discord.
#[derive(Clone)]
struct User {
    id: String,
    name: String,
    avatar: String,
}

/// Configuração do login com Discord (OAuth2). Ausente = login desligado.
struct Discord {
    client_id: String,
    client_secret: String,
    /// Se definido, só entra quem é membro deste servidor do Discord.
    guild: Option<String>,
    api: String,
    authorize_url: String,
    /// Endereço público da página, sem barra final (ex.: https://host/tela).
    public_url: String,
    cookie_path: String,
    secure: bool,
}

impl Discord {
    fn from_env() -> Option<Self> {
        let client_id = std::env::var("DISCORD_CLIENT_ID").ok().filter(|v| !v.is_empty())?;
        let client_secret = std::env::var("DISCORD_CLIENT_SECRET")
            .expect("DISCORD_CLIENT_SECRET é obrigatório quando DISCORD_CLIENT_ID está definido");
        let public_url = std::env::var("PUBLIC_URL")
            .expect("PUBLIC_URL é obrigatório para o login com Discord (ex.: https://host/tela)")
            .trim_end_matches('/')
            .to_string();
        let path = reqwest::Url::parse(&public_url).expect("PUBLIC_URL inválido").path().to_string();
        Some(Self {
            client_id,
            client_secret,
            guild: std::env::var("DISCORD_GUILD_ID").ok().filter(|v| !v.is_empty()),
            api: env_or("DISCORD_API", "https://discord.com/api"),
            authorize_url: env_or("DISCORD_AUTHORIZE_URL", "https://discord.com/oauth2/authorize"),
            secure: public_url.starts_with("https://"),
            cookie_path: if path.is_empty() { "/".into() } else { path },
            public_url,
        })
    }

    fn redirect_uri(&self) -> String {
        format!("{}/auth/callback", self.public_url)
    }

    fn cookie(&self, value: &str, max_age: u64) -> String {
        format!(
            "{SESSION_COOKIE}={value}; Path={}; Max-Age={max_age}; HttpOnly; SameSite=Lax{}",
            self.cookie_path,
            if self.secure { "; Secure" } else { "" }
        )
    }
}

const SESSION_COOKIE: &str = "tela_session";
const SESSION_TTL: Duration = Duration::from_secs(30 * 24 * 3600);
const LOGIN_TTL: Duration = Duration::from_secs(600);

struct Peer {
    role: Role,
    name: String,
    avatar: String,
    guest: bool,
    client: String,
    tx: mpsc::Sender<String>,
}

impl Peer {
    /// Quem compartilha e quem assiste devem ser ligados: papéis opostos e abas diferentes.
    fn pairs_with(&self, other: &Peer) -> bool {
        self.role != other.role && self.client != other.client
    }
}

/// Alguém esperando o dono permitir a entrada.
struct Pending {
    name: String,
    avatar: String,
    guest: bool,
    client: String,
    decide: oneshot::Sender<bool>,
}

#[derive(Default)]
struct Room {
    /// Todas as conexões na sala. Várias pessoas podem compartilhar ao mesmo tempo.
    peers: HashMap<u64, Peer>,
    /// `client` de quem criou a sala: é quem responde os pedidos de entrada.
    owner: String,
    access: Access,
    pass: String,
    /// Aparece em "Ao vivo agora".
    listed: bool,
    /// Abas que já passaram pela senha/aprovação (não pedem de novo ao compartilhar ou recarregar).
    admitted: HashSet<String>,
    pending: HashMap<u64, Pending>,
}

fn host_info(id: u64, p: &Peer) -> Value {
    json!({ "id": id, "name": p.name, "avatar": p.avatar })
}

fn joined_msg(id: u64, p: &Peer) -> Value {
    json!({ "type": "viewer-joined", "id": id, "name": p.name, "avatar": p.avatar })
}

fn request_msg(id: u64, p: &Pending) -> Value {
    json!({ "type": "request", "id": id, "name": p.name, "avatar": p.avatar, "guest": p.guest })
}

impl Room {
    /// Conexões de presença (as de quem assiste) do dono da sala.
    fn owner_peers(&self) -> impl Iterator<Item = &Peer> {
        self.peers.values().filter(|p| p.role == Role::Viewer && p.client == self.owner)
    }

    /// Se o dono saiu, a pessoa há mais tempo na sala assume (e recebe os pedidos pendentes).
    fn ensure_owner(&mut self) {
        if self.peers.values().any(|p| p.client == self.owner) {
            return;
        }
        let Some(oldest) = self.peers.iter().min_by_key(|(id, _)| **id).map(|(_, p)| p.client.clone()) else {
            return;
        };
        self.admitted.insert(oldest.clone());
        self.owner = oldest;
        for (pid, pend) in &self.pending {
            for p in self.owner_peers() {
                send(p, request_msg(*pid, pend));
            }
        }
    }

    /// Lista de pessoas (uma por aba), na ordem de chegada, vista por `me`.
    fn members_for(&self, me: &str) -> Vec<Value> {
        // client -> (primeira conexão, id de quem compartilha)
        let mut by: HashMap<&str, (u64, &Peer, Option<u64>)> = HashMap::new();
        for (id, p) in &self.peers {
            let e = by.entry(p.client.as_str()).or_insert((*id, p, None));
            if *id < e.0 {
                e.0 = *id;
                e.1 = p;
            }
            if p.role == Role::Host {
                e.2 = Some(*id);
            }
        }
        let mut list: Vec<_> = by.into_iter().collect();
        list.sort_by_key(|(_, (first, _, _))| *first);
        list.into_iter()
            .map(|(client, (_, p, host))| {
                json!({
                    "name": p.name, "avatar": p.avatar, "guest": p.guest,
                    "sharing": host.is_some(), "hostId": host,
                    "self": client == me, "owner": client == self.owner,
                })
            })
            .collect()
    }

    /// Manda a lista de pessoas atualizada para todo mundo na sala.
    fn broadcast_members(&self) {
        for p in self.peers.values().filter(|p| p.role == Role::Viewer) {
            send(p, json!({ "type": "members", "list": self.members_for(&p.client), "access": self.access.as_str() }));
        }
    }
}

#[derive(Default)]
struct AppState {
    rooms: Mutex<HashMap<String, Room>>,
    next_id: AtomicU64,
    max_viewers: usize,
    /// Quantas pessoas podem compartilhar a tela ao mesmo tempo na mesma sala.
    max_hosts: usize,
    discord: Option<Discord>,
    http: reqwest::Client,
    /// token do cookie -> (usuário, validade). Em memória: reiniciar o servidor pede novo login.
    sessions: Mutex<HashMap<String, (User, Instant)>>,
    /// `state` do OAuth -> (para onde voltar, quando começou)
    logins: Mutex<HashMap<String, (String, Instant)>>,
}

type Shared = Arc<AppState>;

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn send(peer: &Peer, v: Value) {
    // try_send: se o cliente estiver travado, descarta em vez de acumular RAM.
    let _ = peer.tx.try_send(v.to_string());
}

/// Corta em `max` caracteres e remove controles; o texto vai para outro navegador.
fn clean(s: &str, max: usize) -> String {
    s.chars().filter(|c| !c.is_control()).take(max).collect::<String>().trim().to_string()
}

fn valid_room(room: &str) -> bool {
    (1..=64).contains(&room.len())
        && room
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let state: Shared = Arc::new(AppState {
        max_viewers: env_or("MAX_VIEWERS", "10").parse().unwrap_or(10),
        max_hosts: env_or("MAX_HOSTS", "6").parse().unwrap_or(6),
        discord: Discord::from_env(),
        ..Default::default()
    });
    match &state.discord {
        Some(d) => eprintln!("login com Discord ligado; redirect URI: {}", d.redirect_uri()),
        None => eprintln!("login com Discord desligado (defina DISCORD_CLIENT_ID para ligar)"),
    }

    let app = Router::new()
        .route("/", get(|| async { Html(INDEX) }))
        .route("/favicon.png", get(favicon))
        .route("/config", get(config))
        .route("/ws", get(ws_handler))
        .route("/me", get(me))
        .route("/rooms", get(rooms_list))
        .route("/auth/login", get(auth_login))
        .route("/auth/callback", get(auth_callback))
        .route("/auth/logout", get(auth_logout))
        .with_state(state);

    let addr = env_or("BIND", "0.0.0.0:8080");
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .expect("não consegui abrir a porta");
    eprintln!("screenshare ouvindo em http://{addr}");
    axum::serve(listener, app).await.expect("erro no servidor");
}

async fn favicon() -> impl IntoResponse {
    (
        [(CONTENT_TYPE, "image/png"), (CACHE_CONTROL, "public, max-age=86400")],
        FAVICON,
    )
}

/// Servidores ICE entregues ao navegador (STUN + TURN opcional via env).
async fn config() -> Json<Value> {
    let mut servers = vec![json!({ "urls": env_or("STUN_URL", "stun:stun.l.google.com:19302") })];
    if let Ok(url) = std::env::var("TURN_URL") {
        servers.push(json!({
            "urls": url.split(',').map(str::trim).collect::<Vec<_>>(),
            "username": env_or("TURN_USER", ""),
            "credential": env_or("TURN_PASS", ""),
        }));
    }
    Json(json!({ "iceServers": servers }))
}

/// 24 bytes aleatórios em hex: token de sessão e `state` do OAuth.
fn token() -> String {
    let mut b = [0u8; 24];
    getrandom::getrandom(&mut b).expect("sem fonte de aleatoriedade");
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn session_token(headers: &HeaderMap) -> Option<&str> {
    let prefix = format!("{SESSION_COOKIE}=");
    headers
        .get(COOKIE)?
        .to_str()
        .ok()?
        .split(';')
        .find_map(|c| c.trim().strip_prefix(prefix.as_str()))
}

fn session_user(state: &AppState, headers: &HeaderMap) -> Option<User> {
    let tok = session_token(headers)?;
    let mut sessions = state.sessions.lock().unwrap();
    match sessions.get(tok) {
        Some((user, expires)) if *expires > Instant::now() => Some(user.clone()),
        Some(_) => {
            sessions.remove(tok);
            None
        }
        None => None,
    }
}

/// Diz à página se o login está ligado e quem está logado.
async fn me(State(state): State<Shared>, headers: HeaderMap) -> Json<Value> {
    let user = session_user(&state, &headers)
        .map(|u| json!({ "id": u.id, "name": u.name, "avatar": u.avatar }));
    Json(json!({ "login": state.discord.is_some(), "user": user }))
}

#[derive(Deserialize)]
struct LoginParams {
    /// Query string da página para onde voltar (ex.: `?room=abc&view=1`).
    #[serde(default)]
    next: String,
}

async fn auth_login(State(state): State<Shared>, Query(p): Query<LoginParams>) -> Response {
    let Some(d) = &state.discord else {
        return StatusCode::NOT_FOUND.into_response();
    };
    // só aceita voltar para a própria página, com uma query string curta
    let next = if p.next.starts_with('?') && p.next.len() <= 512 && !p.next.chars().any(char::is_control) {
        p.next
    } else {
        String::new()
    };
    let st = token();
    {
        let mut logins = state.logins.lock().unwrap();
        logins.retain(|_, (_, t)| t.elapsed() < LOGIN_TTL);
        if logins.len() >= 10_000 {
            logins.clear();
        }
        logins.insert(st.clone(), (next, Instant::now()));
    }
    let scope = if d.guild.is_some() { "identify guilds" } else { "identify" };
    let url = reqwest::Url::parse_with_params(
        &d.authorize_url,
        &[
            ("client_id", d.client_id.as_str()),
            ("response_type", "code"),
            ("redirect_uri", d.redirect_uri().as_str()),
            ("scope", scope),
            ("state", st.as_str()),
            ("prompt", "none"),
        ],
    )
    .expect("DISCORD_AUTHORIZE_URL inválido");
    Redirect::to(url.as_str()).into_response()
}

#[derive(Deserialize)]
struct CallbackParams {
    #[serde(default)]
    code: String,
    #[serde(default)]
    state: String,
}

async fn auth_callback(State(state): State<Shared>, Query(p): Query<CallbackParams>) -> Response {
    let Some(d) = &state.discord else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let fail = |why: &str| Redirect::to(&format!("{}/?erro={why}", d.public_url)).into_response();

    let pending = state.logins.lock().unwrap().remove(&p.state);
    let Some((next, started)) = pending else { return fail("login") };
    if started.elapsed() > LOGIN_TTL || p.code.is_empty() {
        return fail("login"); // expirou, ou a pessoa cancelou na tela do Discord
    }

    let user = match discord_user(&state, d, &p.code).await {
        Ok(u) => u,
        Err(why) => return fail(why),
    };

    let tok = token();
    {
        let mut sessions = state.sessions.lock().unwrap();
        let now = Instant::now();
        sessions.retain(|_, (_, expires)| *expires > now);
        sessions.insert(tok.clone(), (user, now + SESSION_TTL));
    }
    let mut res = Redirect::to(&format!("{}/{next}", d.public_url)).into_response();
    if let Ok(v) = d.cookie(&tok, SESSION_TTL.as_secs()).parse() {
        res.headers_mut().insert(SET_COOKIE, v);
    }
    res
}

/// Troca o `code` por um token e busca o perfil (e, se configurado, confere o servidor).
async fn discord_user(state: &AppState, d: &Discord, code: &str) -> Result<User, &'static str> {
    let redirect = d.redirect_uri();
    let tok: Value = state
        .http
        .post(format!("{}/oauth2/token", d.api))
        .form(&[
            ("client_id", d.client_id.as_str()),
            ("client_secret", d.client_secret.as_str()),
            ("grant_type", "authorization_code"),
            ("code", code),
            ("redirect_uri", redirect.as_str()),
        ])
        .send()
        .await
        .map_err(|_| "login")?
        .json()
        .await
        .map_err(|_| "login")?;
    let access = tok["access_token"].as_str().ok_or("login")?;

    let get = |path: &str| state.http.get(format!("{}{path}", d.api)).bearer_auth(access).send();
    let me: Value = get("/users/@me").await.map_err(|_| "login")?.json().await.map_err(|_| "login")?;
    let id = me["id"].as_str().ok_or("login")?.to_string();

    if let Some(guild) = &d.guild {
        let guilds: Value = get("/users/@me/guilds")
            .await
            .map_err(|_| "login")?
            .json()
            .await
            .map_err(|_| "login")?;
        let member = guilds
            .as_array()
            .is_some_and(|gs| gs.iter().any(|g| g["id"].as_str() == Some(guild.as_str())));
        if !member {
            return Err("servidor");
        }
    }

    let name = me["global_name"].as_str().or(me["username"].as_str()).unwrap_or("Discord");
    let avatar = match me["avatar"].as_str() {
        Some(hash) if hash.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') => {
            format!("https://cdn.discordapp.com/avatars/{id}/{hash}.png?size=64")
        }
        _ => String::new(),
    };
    Ok(User { name: clean(name, 40), avatar, id })
}

async fn auth_logout(State(state): State<Shared>, headers: HeaderMap) -> Response {
    let Some(d) = &state.discord else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if let Some(tok) = session_token(&headers) {
        state.sessions.lock().unwrap().remove(tok);
    }
    let mut res = Redirect::to(&format!("{}/", d.public_url)).into_response();
    if let Ok(v) = d.cookie("", 0).parse() {
        res.headers_mut().insert(SET_COOKIE, v);
    }
    res
}

/// Salas visíveis em que alguém está compartilhando ("Ao vivo agora" na tela inicial).
async fn rooms_list(State(state): State<Shared>) -> Json<Value> {
    let rooms = state.rooms.lock().unwrap();
    let mut list: Vec<Value> = rooms
        .iter()
        .filter(|(_, r)| r.listed)
        .filter_map(|(code, r)| {
            let mut hosts: Vec<_> = r.peers.iter().filter(|(_, p)| p.role == Role::Host).collect();
            if hosts.is_empty() {
                return None;
            }
            hosts.sort_by_key(|(id, _)| **id);
            let people = r.peers.values().map(|p| p.client.as_str()).collect::<HashSet<_>>().len();
            let sharers: Vec<Value> = hosts
                .iter()
                .take(4)
                .map(|(_, p)| json!({ "name": p.name, "avatar": p.avatar }))
                .collect();
            Some(json!({
                "code": code, "access": r.access.as_str(), "people": people,
                "sharing": hosts.len(), "sharers": sharers,
            }))
        })
        .take(30)
        .collect();
    list.sort_by(|a, b| b["people"].as_u64().cmp(&a["people"].as_u64()));
    Json(json!({ "rooms": list }))
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    Query(mut params): Query<JoinParams>,
    State(state): State<Shared>,
    headers: HeaderMap,
) -> Response {
    // Com sessão do Discord, o nick e o avatar vêm da conta, nunca do cliente.
    // Sem sessão a pessoa entra como convidada, com o nick que digitou.
    match session_user(&state, &headers) {
        Some(user) => {
            params.name = user.name;
            params.avatar = user.avatar;
        }
        None => params.guest = state.discord.is_some(),
    }
    if !valid_room(&params.room) {
        return (StatusCode::BAD_REQUEST, "nome de sala inválido").into_response();
    }
    ws.max_message_size(64 * 1024)
        .on_upgrade(move |socket| peer_session(socket, params, state))
        .into_response()
}

/// Escrita: fila -> socket, com ping para manter proxies vivos. Fecha o socket quando a fila acaba.
async fn write_loop(mut sink: SplitSink<WebSocket, Message>, mut rx: mpsc::Receiver<String>) {
    let mut ping = tokio::time::interval(Duration::from_secs(25));
    ping.tick().await;
    loop {
        tokio::select! {
            msg = rx.recv() => match msg {
                Some(m) => if sink.send(Message::Text(m.into())).await.is_err() { return },
                None => break,
            },
            _ = ping.tick() => {
                if sink.send(Message::Ping(Default::default())).await.is_err() { return }
            }
        }
    }
    let _ = sink.close().await;
}

/// Termina quando o navegador fecha a conexão (usado enquanto a pessoa espera aprovação).
async fn closed(stream: &mut SplitStream<WebSocket>) {
    while let Some(Ok(msg)) = stream.next().await {
        if matches!(msg, Message::Close(_)) {
            break;
        }
    }
}

/// Resultado da portaria da sala.
enum Gate {
    Joined,
    Deny(Value),
    Wait(oneshot::Receiver<bool>),
}

fn deny(message: &str) -> Value {
    json!({ "type": "error", "message": message })
}

/// Coloca a conexão na sala e avisa todo mundo. Chamada com o lock das salas.
fn join(state: &AppState, room: &mut Room, id: u64, me: Peer) -> Result<(), &'static str> {
    let count = |role| room.peers.values().filter(|p| p.role == role).count();
    if me.role == Role::Host && count(Role::Host) >= state.max_hosts {
        return Err("Já tem gente demais compartilhando nesta sala.");
    }
    if me.role == Role::Viewer && count(Role::Viewer) >= state.max_viewers {
        return Err("Sala lotada.");
    }
    let others = || room.peers.iter().filter(|(_, p)| me.pairs_with(p));
    let hosts: Vec<Value> = match me.role {
        Role::Viewer => others().map(|(pid, p)| host_info(*pid, p)).collect(),
        Role::Host => Vec::new(),
    };
    let is_owner = me.client == room.owner;
    send(&me, json!({ "type": "welcome", "id": id, "hosts": hosts, "owner": is_owner, "access": room.access.as_str() }));

    match me.role {
        // avisa quem compartilha de cada espectador presente, e cada espectador da nova tela
        Role::Host => {
            let mut info = host_info(id, &me);
            info["type"] = json!("host-joined");
            for (pid, p) in others() {
                send(&me, joined_msg(*pid, p));
                send(p, info.clone());
            }
        }
        Role::Viewer => {
            for (_, h) in others() {
                send(h, joined_msg(id, &me));
            }
            // o dono voltou (recarregou a página): reenvia os pedidos que estavam esperando
            if is_owner {
                for (pid, pend) in &room.pending {
                    send(&me, request_msg(*pid, pend));
                }
            }
        }
    }
    room.peers.insert(id, me);
    room.broadcast_members();
    Ok(())
}

async fn peer_session(socket: WebSocket, mut params: JoinParams, state: Shared) {
    let id = state.next_id.fetch_add(1, Ordering::Relaxed) + 1;
    params.name = clean(&params.name, 40);
    if params.name.is_empty() {
        params.name = "Convidado".into();
    }
    params.client = clean(&params.client, 64);
    if params.client.is_empty() {
        params.client = format!("c{id}");
    }
    // para o registro de entradas e saídas (docker compose logs -t screenshare)
    let nick = params.name.clone();
    let papel = if params.role == Role::Host { "compartilhando" } else { "assistindo" };
    let started = Instant::now();

    let (tx, rx) = mpsc::channel::<String>(256);
    let (sink, mut stream) = socket.split();
    let writer = tokio::spawn(write_loop(sink, rx));
    let peer = |params: &JoinParams, tx: &mpsc::Sender<String>| Peer {
        role: params.role,
        name: params.name.clone(),
        avatar: params.avatar.clone(),
        guest: params.guest,
        client: params.client.clone(),
        tx: tx.clone(),
    };

    // ---- portaria (bloco síncrono: o lock nunca atravessa um .await) ----
    let gate = {
        let mut rooms = state.rooms.lock().unwrap();
        let room = rooms.entry(params.room.clone()).or_default();
        if room.owner.is_empty() {
            // sala nova: quem chega primeiro é o dono e define as regras
            room.owner = params.client.clone();
            room.access = Access::parse(&params.access);
            room.pass = clean(&params.pass, 64);
            room.listed = params.unlisted != "1";
            if room.access == Access::Password && room.pass.is_empty() {
                room.access = Access::Open;
            }
        }
        let inside = params.client == room.owner || room.admitted.contains(&params.client);
        let verdict = if inside {
            Ok(())
        } else {
            match room.access {
                Access::Open => Ok(()),
                Access::Discord if params.guest => {
                    Err(Gate::Deny(json!({ "type": "error", "code": "discord", "message": "Esta sala só aceita quem entrou com Discord." })))
                }
                Access::Discord => Ok(()),
                Access::Password if clean(&params.key, 64) == room.pass => Ok(()),
                Access::Password => {
                    let message = if params.key.is_empty() { "Digite a senha da sala." } else { "Senha incorreta. Tente de novo." };
                    Err(Gate::Deny(json!({ "type": "error", "code": "password", "message": message })))
                }
                Access::Approve if room.pending.len() >= 20 => {
                    Err(Gate::Deny(deny("Muita gente pedindo para entrar. Tente daqui a pouco.")))
                }
                Access::Approve => {
                    room.ensure_owner();
                    let (decide, decision) = oneshot::channel();
                    let pend = Pending {
                        name: params.name.clone(),
                        avatar: params.avatar.clone(),
                        guest: params.guest,
                        client: params.client.clone(),
                        decide,
                    };
                    for p in room.owner_peers() {
                        send(p, request_msg(id, &pend));
                    }
                    room.pending.insert(id, pend);
                    Err(Gate::Wait(decision))
                }
            }
        };
        match verdict {
            Ok(()) => {
                room.admitted.insert(params.client.clone());
                match join(&state, room, id, peer(&params, &tx)) {
                    Ok(()) => Gate::Joined,
                    Err(msg) => Gate::Deny(deny(msg)),
                }
            }
            Err(gate) => gate,
        }
    };

    let refusal: Option<Value> = match gate {
        Gate::Joined => None,
        Gate::Deny(v) => Some(v),
        Gate::Wait(decision) => {
            eprintln!("sala={} nick={nick:?} pediu para entrar", params.room);
            let _ = tx.try_send(json!({ "type": "waiting" }).to_string());
            let answer = tokio::select! {
                d = decision => Some(d),
                _ = closed(&mut stream) => None,
            };
            let mut rooms = state.rooms.lock().unwrap();
            match (answer, rooms.get_mut(&params.room)) {
                (Some(Ok(true)), Some(room)) => join(&state, room, id, peer(&params, &tx)).err().map(deny),
                (Some(Ok(false)), _) => Some(deny("O dono da sala não permitiu a entrada.")),
                (Some(_), _) => Some(deny("A sala fechou antes de alguém responder.")),
                (None, room) => {
                    // desistiu de esperar: tira o pedido da tela do dono
                    if let Some(room) = room {
                        if room.pending.remove(&id).is_some() {
                            for p in room.owner_peers() {
                                send(p, json!({ "type": "request-gone", "id": id }));
                            }
                        }
                    }
                    Some(Value::Null)
                }
            }
        }
    };

    if let Some(v) = refusal {
        if !v.is_null() {
            eprintln!("sala={} nick={nick:?} recusado ({papel}): {}", params.room, v["message"].as_str().unwrap_or(""));
            let _ = tx.try_send(v.to_string());
        }
        drop(tx);
        let _ = tokio::time::timeout(Duration::from_secs(3), writer).await;
        // remove sala vazia criada pelo entry()
        let mut rooms = state.rooms.lock().unwrap();
        if rooms.get(&params.room).is_some_and(|r| r.peers.is_empty()) {
            rooms.remove(&params.room);
        }
        return;
    }
    eprintln!("sala={} nick={nick:?} entrou {papel}", params.room);

    // ---- leitura: repassa sinalização e respostas do dono ----
    while let Some(Ok(msg)) = stream.next().await {
        match msg {
            Message::Text(text) => route(&state, &params.room, id, text.as_str()),
            Message::Close(_) => break,
            _ => {}
        }
    }

    // ---- saída ----
    {
        let mut rooms = state.rooms.lock().unwrap();
        let mut empty = false;
        if let Some(room) = rooms.get_mut(&params.room) {
            if let Some(me) = room.peers.remove(&id) {
                let kind = if me.role == Role::Host { "host-left" } else { "viewer-left" };
                for p in room.peers.values().filter(|p| me.pairs_with(p)) {
                    send(p, json!({ "type": kind, "id": id }));
                }
            }
            empty = room.peers.is_empty();
            if !empty {
                // com pedidos esperando, alguém precisa poder responder
                if !room.pending.is_empty() {
                    room.ensure_owner();
                }
                room.broadcast_members();
            }
        }
        if empty {
            // quem estava esperando aprovação recebe "a sala fechou"
            rooms.remove(&params.room);
        }
    }
    writer.abort();
    eprintln!(
        "sala={} nick={nick:?} saiu ({papel}) depois de {}s",
        params.room,
        started.elapsed().as_secs()
    );
}

/// Mensagens que chegam de quem já está na sala:
/// - `{type:"signal", to, data}` -> `{type:"signal", from, data}`, só entre quem compartilha
///   e quem assiste (papéis opostos) da mesma sala;
/// - `{type:"admit", id, allow}`: o dono responde um pedido de entrada.
fn route(state: &AppState, room: &str, from: u64, text: &str) {
    let Ok(v) = serde_json::from_str::<Value>(text) else { return };
    let mut rooms = state.rooms.lock().unwrap();
    let Some(r) = rooms.get_mut(room) else { return };
    let Some(me) = r.peers.get(&from) else { return };

    if v["type"] == "signal" {
        let Some(target) = v["to"].as_u64().and_then(|to| r.peers.get(&to)) else { return };
        if me.pairs_with(target) {
            send(target, json!({ "type": "signal", "from": from, "data": v["data"] }));
        }
    } else if v["type"] == "admit" && me.client == r.owner {
        let Some(client) = v["id"].as_u64().and_then(|id| r.pending.get(&id)).map(|p| p.client.clone()) else { return };
        let allow = v["allow"].as_bool().unwrap_or(false);
        if allow {
            r.admitted.insert(client.clone());
        }
        // responde todos os pedidos da mesma aba de uma vez
        let ids: Vec<u64> = r.pending.iter().filter(|(_, p)| p.client == client).map(|(id, _)| *id).collect();
        for id in ids {
            if let Some(p) = r.pending.remove(&id) {
                let _ = p.decide.send(allow);
            }
            for o in r.owner_peers() {
                send(o, json!({ "type": "request-gone", "id": id }));
            }
        }
    }
}
