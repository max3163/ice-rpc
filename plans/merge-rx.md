# Plan — Fusionner `ice-rpc-rx` dans `ice-rpc` et simplifier l'API de flux

> Suite de [`plans/simplification.md`](simplification.md) et
> [`plans/transport-channels.md`](transport-channels.md).
> Décision validée : les opérateurs deviennent des **méthodes inhérentes** sur
> `Observable`, au prix d'un `Box<dyn Stream>` par étape.

## 1. Constat

- **`ice-rpc-rx` est `publish = false`** ([Cargo.toml](../ice-rpc-rx/Cargo.toml:8)) :
  la fusion est purement interne, **aucune compatibilité externe à préserver**.
  Le principal risque d'un tel refactor disparaît.
- Contenu du crate : le trait `RxStreamExt` (22 méthodes,
  [transform/mod.rs](../ice-rpc-rx/src/transform/mod.rs:36)), ~20 **types
  d'opérateurs publics** (`Map`, `Filter`, `Take`, `Skip`, `First`, `MapErr`,
  `Scan`, `Tap`, `Finalize`, `Delay`, `Timeout`, `SwitchMap`, `TakeUntil`,
  `StartWith`, `Merge`, `Retry`, `CatchError`), 3 constructeurs (`from`, `of`,
  `throw_error`), 4 items de combinaison (`merge`, `retry`, `retry_with`,
  `retry_with_delay`), 5 items de push (`Observer`, `ObserverFns`,
  `Subscription`, `Subject`, `ShareReplay`) — **et les 5 exemples
  d'application** (`provider-app`, `consumer-app`, `benchmark-app`,
  `consumer-http-app`, `state_service`) avec `common` en dev-dependency.
- **Doublon structurel** : `Observable::first_value` / `collect` (inhérents,
  [stream.rs:242](../ice-rpc/src/types/stream.rs:242)) **et**
  `RxStreamExt::first_value` / `collect` — le code l'assume et le documente
  ([stream.rs:263](../ice-rpc/src/types/stream.rs:263)), avec un test qui vérifie
  que les deux surfaces s'accordent.
- **`into_observable()` existe par contrainte de typage** : un pipeline rend un
  `Map<…>`, pas un `Observable`, donc il faut le « geler » pour le retourner d'une
  méthode de service ; le workaround `Observable::from_stream(...)` est documenté
  ([stream.rs:96-117](../ice-rpc/src/types/stream.rs:96)).
- **La cible est déjà à moitié construite** : `StreamInner::Pipeline { stream:
  Pin<Box<dyn futures_lite::Stream<Item = Event<T, E>> + Send>>, pending }`
  ([stream.rs:40](../ice-rpc/src/types/stream.rs:40)) et
  `Observable::from_stream` ([stream.rs:119](../ice-rpc/src/types/stream.rs:119)) ;
  le repli `Next + Complete → CompleteWith` est conservé dans cette variante.
- Les dépendances nécessaires (`futures-lite`, `pin-project-lite`, `futures-timer`,
  `async-channel`, `async-lock`) sont **déjà** dans `ice-rpc` : la fusion
  n'ajoute aucune dépendance.

## 2. Décisions

| Décision | Conséquence |
|---|---|
| Opérateurs = **méthodes inhérentes** sur `Observable`, retournant `Observable` via `from_stream` | **1 type public** ; un `Box<dyn Stream>` par étape (pas de canal) ; `into_observable` et `RxStreamExt` disparaissent |
| **Un seul type d'erreur** : `StreamError` fusionne dans `ObservableError` (`Business` / `Technical` / `Empty`) | `first_value()` et `collect()` renvoient le même type |
| **`Event` et `recv()` restent publics** : le code spécifique sur `Next` / `Error` / `Complete` reste possible, comme en RxJS. `recv_wire()` passe `pub(crate)` et `next() -> Option<Result<T, ObservableError<E>>>` est ajouté comme vue concise, implémentée **au-dessus** de `recv()` (pas une seconde implémentation) | Le vocabulaire Rx est disponible sur les deux axes (pull et push) ; `WireEvent`/`Sender` restent dans `gen` (contrat macros), `recv_wire` quitte l'API |
| *Alternative* (à trancher) : cacher `Event` et ne garder que `next()` | API minimale, mais « complete » s'écrit `None` et le vocabulaire `Complete` disparaît de l'axe pull — recommandé de garder `Event` public |
| **Push unifié, contrat Rx conservé** : `Subject` (avec `replay: usize`, absorbant `ShareReplay`) et **deux** points d'entrée — `subscribe(next)` et `subscribe_all(next, error, complete)` | Le vocabulaire `next` / `error` / `complete` est conservé ; `Observer` (trait) et `ObserverFns` passent en interne, `subscribe_with` disparaît, `Subscription` inchangé |
| Les **exemples sortent** dans `ice-rpc/examples` ; `common` devient dev-dependency ; le crate `ice-rpc-rx` est **supprimé** | `ice-rpc/Cargo.toml` gagne `common` (dev) et 5 `[[example]] required-features` |
| Paramètres morts de `#[service]` supprimés (`allow_large_payload`, `default_size_message`, `discovery_timeout`) | 3 knobs fantômes en moins dans le parseur, le codegen et les 4 Readmes |

Note sur l'axe **pull** : les deux vues (`recv()` détaillée, `next()` concise)
doivent rester **totales**, jamais un `None` ambigu. Une fin normale donne
`None` (ou `Event::Complete`), une erreur terminale donne `Some(Err(..))` (ou
`Event::Error(..)`), et une fermeture **abrupte** (fournisseur disparu) doit
produire une erreur **technique** avant la fin, sinon l'appelant ne peut plus
distinguer « c'est fini » de « ça a lâché ». Le transport sait déjà produire cet
événement (`RpcError::TransportError`), il suffit de ne pas le perdre dans
l'adaptateur.

Correspondance des deux axes, une fois cette règle posée — le code de fin de
flux reste possible partout :

| RxJS | Push (`subscribe_all`) | Pull détaillé (`recv`) | Pull concis (`next`) |
|---|---|---|---|
| `next` | 1er rappel | `Event::Next(v)` | `Some(Ok(v))` |
| `error` | 2e rappel | `Event::Error(e)` | `Some(Err(e))` |
| `complete` | 3e rappel | `Event::Complete` | `None` |
| `finalize` | `.finalize(..)` (toute terminaison) | idem | idem |
| `unsubscribe` | `Subscription::unsubscribe()` / drop | drop de l'`Observable` | idem |

`None` signifie donc **complete** — le producteur a terminé proprement — et jamais
« ça a lâché » : c'est ce qui rend le code de fin de flux fiable, que l'on écrive
le 3e rappel ou la branche `None`.

Points ouverts, tranchés pendant l'exécution (étape 4/5), en s'appuyant sur les
usages réels des exemples :

- `map_err` : utile quand la source a un type d'erreur à transformer — à garder
  seulement si `switch_map`/`merge` l'exigent ;
- ~~`subscribe`~~ **tranché** : le contrat RxJS est conservé, exposé par deux
  méthodes seulement — `subscribe(|v| ...)` pour la valeur seule, et
  `subscribe_all(|v| ..., |e| ..., || ...)` pour `next` / `error` / `complete` ;
- `retry` / `retry_with` / `retry_with_delay` : **aucun usage** dans les exemples
  → ne garder qu'une variante (factory + prédicat + délai optionnel), ou aucune ;
- `merge` : aucun usage dans les exemples → candidat à la suppression pure.

## 3. Cible (API utilisateur)

```rust
// Un seul type, tout dessus : plus d'import de trait, plus d'into_observable.
let ages = db.get_user_age("Alice".into()).await   // Observable<i32, DbError>
    .filter(|v| *v > 0)
    .map(|v| v * 2)
    .take(5);

let first = ages.first_value().await?;             // Result<T, ObservableError<E>>
let all   = db.list().await.collect().await?;      // Vec<T>
db.watch().await
    .timeout(Duration::from_millis(500))
    .for_each(|v| log::info!("{v}"))
    .await?;

// Push : une seule primitive, un seul handle d'annulation, contrat Rx conservé.
let state = Subject::<Status, String>::replay(1);  // ex-ShareReplay
let stream = state.subscribe();                    // Observable
state.next(status).await;

// Vue valeur seule…
let sub = stream.subscribe(|v| log::info!("{v}"));
// …ou les trois rappels, comme subscribe({ next, error, complete }) en RxJS.
let sub = stream.subscribe_all(
    |v| log::info!("{v}"),
    |e: ObservableError<DbError>| log::error!("{e}"),
    || log::info!("terminé"),
);
sub.closed().await;
```

## 4. Étapes (chacune compile, suite verte, commit séparé)

Avancement : 1 ✅ (`cab7b02`), 2 ✅ (`0bda2d8`), 3 ✅ (commit de cette étape),
4 à 6 à faire.

1. **Fusion mécanique** — déplacer `ice-rpc-rx/src/{creation.rs, subject.rs,
   share_replay.rs, subscribe.rs, transform/}` vers `ice-rpc/src/rx/`, puis
   `pub use rx::*;` au root. Aucune API ne change : les ~800 lignes de tests du
   crate Rx passent telles quelles dans `ice-rpc`.
   *Validation :* `cargo test -p ice-rpc` (les tests Rx sont désormais couverts
   par ce crate), `cargo clippy --workspace --all-targets`.
2. **Sortie des exemples** — `ice-rpc-rx/examples/*` → `ice-rpc/examples/*`,
   `use ice_rpc_rx::` → `use ice_rpc::`, ajout de la dev-dependency
   `common = { path = "../examples/common" }` et des `[[example]]`
   `required-features = ["tokio"]`. Mettre à jour `Makefile.toml`
   (`EXAMPLES_CRATE`), les 11 alias de [`.cargo/config.toml`](../.cargo/config.toml:18),
   [`scripts/bench-load.sh`](../scripts/bench-load.sh:38) (`-p ice-rpc`), la
   racine `Cargo.toml` (members) et `Cargo.lock`. Puis **supprimer le crate**.
   *Validation :* les 5 exemples compilent et tournent (`provider-app` +
   `benchmark-app` en release, 3 modes à 100 %).
3. **Un seul type d'erreur + `next()`** — `StreamError` fusionne dans
   `ObservableError::Empty` ; `Event` et `recv()` **restent publics** (décision
   §2), `next() -> Option<Result<T, ObservableError<E>>>` est ajouté comme vue
   concise **au-dessus** de `recv()`, et une fermeture abrupte y devient une
   erreur technique (jamais un `None` ambigu) ; `recv_wire()` passe `pub(crate)`
   et redevient utilisé — [`observable_to_responses`](../ice-rpc/src/transport.rs:227)
   le branche pour que le provider conserve l'optimisation `CompleteWith`
   (une réponse à valeur unique = **un** échantillon) ; `first_event` /
   `collect_values` deviennent des détails internes ; migration des exemples,
   tests et de [`gen.rs`](../ice-rpc/src/gen.rs:44).
   *Validation :* suite verte (148 tests lib), bench 3 modes à 100 % (0 erreur),
   p50 séquentiel 7 µs.
4. **Opérateurs inhérents** — `Observable::map`, `filter`, `take`, `skip`,
   `first`, `first_with`, `start_with`, `scan`, `tap`, `finalize`,
   `catch_error`, `delay`, `timeout`, `switch_map`, `take_until` retournant
   `Observable` (via `from_stream`) ; les structs d'opérateurs deviennent
   **privées** ; suppression de `RxStreamExt`, `into_observable` et de l'`impl
   futures_lite::Stream for Observable` public (adaptateur interne si encore
   nécessaire).
   *Validation :* nouveau bench criterion `ice-rpc/benches/pipeline.rs`
   (`map`/`filter`/`take` sur 100 000 événements) comparant le chaînage
   `Box<dyn Stream>` à un appel direct — le chiffre documente le coût du boxing.
5. **Push unifié, contrat Rx conservé** — `Subject<T, E>` avec `replay: usize`
   (absorbe `ShareReplay`) ; les trois rappels **restent**, exposés par deux
   méthodes : `subscribe(next)` et `subscribe_all(next, error, complete)`.
   `Observer` (trait) et `ObserverFns` deviennent des détails internes — plus à
   les nommer côté utilisateur —, `subscribe_with` disparaît, `Subscription`
   (annulation au drop, `unsubscribe()`, `closed()`) est inchangé. Suppression de
   `ShareReplay` et de `retry*` / `merge` si les usages confirment qu'ils sont
   morts ; migration de `state_service` et `consumer-app`.
6. **Macro et documentation** — retirer `allow_large_payload`,
   `default_size_message` et `discovery_timeout` de
   [`ServiceAttr`](../ice-rpc-macros/src/lib.rs:51) et du codegen ; supprimer le
   test compile-fail `invalid_discovery_timeout` ; réécrire la doc de flux
   (`Readme.md`, `ice-rpc/Readme.md`, `ice-rpc-macros/Readme.md`), fusionner
   `ice-rpc-rx/Readme.md` dans `Readme.md`, et réaligner les commentaires de
   `gen.rs`, `lib.rs`, `types/mod.rs`, `types/wire.rs`,
   [`gen_contract.rs`](../ice-rpc-macros-tests/tests/gen_contract.rs:1).

## 5. Impact par fichier

| Fichier | Nature du changement |
|---|---|
| `Cargo.toml` (workspace) | retirer `ice-rpc-rx` des membres ; `Cargo.lock` régénéré |
| `ice-rpc/Cargo.toml` | + `common` (dev), + 5 `[[example]]`, + `[[bench]] pipeline` |
| `ice-rpc/src/rx/**` | ex-`ice-rpc-rx/src/**` (création, push, opérateurs, tests) |
| `ice-rpc/src/types/stream.rs` | opérateurs inhérents, `next()`, erreurs fusionnées |
| `ice-rpc/src/types/wire.rs` | `Event`/`ObservableError` : visibilité resserrée |
| `ice-rpc/src/gen.rs` | re-exports Rx retirés, doc « shared with ice-rpc-rx » corrigée |
| `ice-rpc/src/lib.rs`, `types/mod.rs` | ré-exports de l'API de flux |
| `.cargo/config.toml`, `Makefile.toml`, `scripts/bench-load.sh` | `-p ice-rpc-rx` → `-p ice-rpc` |
| `.github/workflows/ci.yml` | ajouter un job qui construit les exemples et teste le façade `tokio` (la CI actuelle ne teste que `-p ice-rpc --lib`) |
| `ice-rpc-macros/src/lib.rs` | `ServiceAttr` réduit |
| `ice-rpc-macros-tests/tests/compile_fail/invalid_discovery_timeout.*` | supprimés |
| `ice-rpc-macros-tests/tests/gen_contract.rs` | contrat `gen` réduit |
| `examples/common/**` | inchangé (aucun usage d'`ice_rpc_rx` vérifié) |
| `gateway_nodejs/**` | à vérifier : dépend d'`ice-rpc` et `common` a priori |
| `Readme.md`, `ice-rpc/Readme.md`, `ice-rpc-macros/Readme.md`, `ice-rpc-rx/Readme.md` | documentation |

## 6. Critères d'acceptation

- Un seul crate publié (`ice-rpc`), un seul type de flux (`Observable`), un seul
  enum d'erreur, **aucun trait d'extension à importer**.
- `cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D warnings`,
  `cargo test --workspace` verts ; les tests Rx tournent dans `ice-rpc`.
- [`scripts/bench-load.sh`](../scripts/bench-load.sh:1) : 3 modes à 100 % de
  succès, p50 séquentiel ≈ 17 µs (pas de régression).
- Le coût du `Box<dyn Stream>` est **mesuré** (bench `pipeline`) et documenté.
- Les 5 exemples compilent et tournent depuis `ice-rpc/examples`.
- Aucune occurrence de `ice_rpc_rx` / `ice-rpc-rx` dans le dépôt (hors git).

## 7. Risques

| Risque | Impact | Mitigation |
|---|---|---|
| ~800 lignes de tests Rx à déplacer | Moyen | étape 1 purement mécanique, **avant** tout changement d'API |
| Sémantique `pending`/`CompleteWith` dans les opérateurs | Moyen | s'appuyer sur `from_stream`, qui gère déjà ce repli |
| Coût du boxing | Faible (décision prise) | bench dédié en étape 4 |
| Suppressions d'API (`Observer`, `subscribe_with`, `ShareReplay`, `retry*`, `merge`) | Faible | recenser les usages réels (5 exemples + tests) avant de supprimer |
| La CI ne couvre ni les exemples ni l'API de flux | Moyen | étape 2 : job dédié |
| `Makefile.toml` / alias `.cargo` cassés silencieusement | Faible | revue exhaustive (`grep ice-rpc-rx`) en fin d'étape 2 |

## 8. Hors périmètre

- Aucun changement de [`transport.rs`](../ice-rpc/src/transport.rs:1) ni du
  format de trame (`RpcHeader`).
- Pas de fusion d'`ice-rpc-macros` (couche compile-time, dépendance dédiée).
- Aucun nouvel opérateur : le plan **réduit** la surface, il ne l'étend pas.
