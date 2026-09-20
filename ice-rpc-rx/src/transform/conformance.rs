//! Matrice de conformité des opérateurs : la spécification exécutable de L5.
//!
//! Chaque opérateur est confronté aux **mêmes huit cas canoniques**, et la
//! sortie est comparée événement par événement, **terminaux inclus**. C'est ce
//! dernier point qui distingue cette matrice des tests existants
//! ([`super::tests`]) : ceux-ci s'arrêtent au premier terminal, alors qu'un
//! opérateur est justement défini par ce qu'il fait d'un terminal — celui qui
//! traverse, celui qu'il rattrape, celui qu'il synthétise, et ce qu'il répond à
//! un poll suivant.
//!
//! # Pourquoi elle a été écrite avant la réécriture
//!
//! Les douze opérateurs couverts par [`event_stream!`](super::event_stream)
//! passent d'un `poll_next` écrit à la main à quatre hooks. La matrice a été
//! écrite **sur le code d'origine**, puis doit rester verte sans modification :
//! c'est ce qui autorise à dire que la factorisation n'a pas changé la
//! sémantique, plutôt que de l'espérer.
//!
//! # Les huit cas
//!
//! | Constructeur | Source |
//! |---|---|
//! | [`no_event`] | un flux vide |
//! | [`one_value`] | `Next(1)`, `Complete` |
//! | [`completion_only`] | `Complete` seul |
//! | [`business_failure`] | `Error(Business)` seul |
//! | [`technical_failure`] | `Error(Technical)` seul |
//! | [`empty_failure`] | `Error(Empty)` seul |
//! | [`abrupt_close`] | `Next(1)` puis fermeture sans terminal |
//! | [`three_values`] | `Next(1)`, `Next(2)`, `Next(3)`, `Complete` |
//!
//! Le cas `empty_failure` mérite d'être signalé : `ObservableError::Empty` est
//! l'artefact de lecture que seul `map_err` transpose, et il est le plus facile
//! à oublier dans un `match` sur les variants d'erreur. Le cas `abrupt_close`
//! exerce le chemin `on_none`, distinct du terminal.

use std::future::poll_fn;

use crate::{Event, Observable, ObservableError, RpcError};

/// Événements d'un cas de la matrice, vu de l'entrée comme de la sortie.
type Sample = Vec<Event<i32, String>>;

/// Un cas de la matrice : les événements de la source, et ceux attendus.
type Case = (Sample, Sample);

// ── Vocabulaire des cas ─────────────────────────────────────────────

/// `Next(1)`.
fn next(value: i32) -> Event<i32, String> {
    Event::Next(value)
}

/// `Complete`.
fn complete() -> Event<i32, String> {
    Event::Complete
}

/// `Error(Business)`, l'erreur écrite par le service.
fn business(message: &str) -> Event<i32, String> {
    Event::Error(ObservableError::Business(message.to_owned()))
}

/// `Error(Technical)`, l'erreur du transport.
fn technical() -> Event<i32, String> {
    Event::Error(ObservableError::Technical(RpcError::Timeout))
}

/// `Error(Empty)`, l'artefact de lecture qu'aucun producteur n'émet.
fn empty() -> Event<i32, String> {
    Event::Error(ObservableError::Empty)
}

/// Un flux sans aucun événement.
fn no_event() -> Vec<Event<i32, String>> {
    Vec::new()
}

/// Une valeur puis la fin normale.
fn one_value() -> Vec<Event<i32, String>> {
    vec![next(1), complete()]
}

/// La fin normale, sans aucune valeur.
fn completion_only() -> Vec<Event<i32, String>> {
    vec![complete()]
}

/// Une erreur métier seule.
fn business_failure() -> Vec<Event<i32, String>> {
    vec![business("boom")]
}

/// Une erreur technique seule.
fn technical_failure() -> Vec<Event<i32, String>> {
    vec![technical()]
}

/// L'erreur `Empty` seule.
fn empty_failure() -> Vec<Event<i32, String>> {
    vec![empty()]
}

/// Une valeur, puis une fermeture **sans** terminal : le chemin `on_none`.
fn abrupt_close() -> Vec<Event<i32, String>> {
    vec![next(1)]
}

/// Trois valeurs puis la fin normale.
fn three_values() -> Vec<Event<i32, String>> {
    vec![next(1), next(2), next(3), complete()]
}

// ── Le vérificateur ─────────────────────────────────────────────────

/// Draine un flux **jusqu'au `None`**, terminaux inclus.
///
/// Les sources de la matrice sont *inline* (aucun canal, aucun runtime) : chaque
/// poll est immédiatement prêt, donc `block_on` sur un `poll_fn` suffit et le
/// test ne dépend d'aucun exécuteur.
fn drain(stream: Observable<i32, String>) -> Sample {
    let mut stream = Box::pin(stream);
    let mut events = Vec::new();
    while let Some(event) = pollster::block_on(poll_fn(|cx| {
        futures_lite::Stream::poll_next(stream.as_mut(), cx)
    })) {
        events.push(event);
    }
    events
}

/// Confronte un opérateur à une table `entrée → sortie attendue`.
///
/// `build` reçoit la source et renvoie le flux transformé. Il est appelé une
/// fois par cas : un opérateur à état ne doit pas voir les cas précédents.
fn assert_matrix(
    name: &str,
    build: impl Fn(Observable<i32, String>) -> Observable<i32, String>,
    cases: &[Case],
) {
    for (input, expected) in cases {
        let produced = drain(build(Observable::from_events(input.clone())));
        assert_eq!(
            &produced, expected,
            "'{name}' : entrée {input:?}\n  attendu : {expected:?}\n  produit : {produced:?}"
        );
    }
}

// ── La matrice, un opérateur à la fois ──────────────────────────────

#[test]
fn map_conformance() {
    assert_matrix(
        "map",
        |stream| stream.map(|v| v * 10),
        &[
            (no_event(), vec![]),
            (one_value(), vec![next(10), complete()]),
            (completion_only(), vec![complete()]),
            (business_failure(), vec![business("boom")]),
            (technical_failure(), vec![technical()]),
            (empty_failure(), vec![empty()]),
            (abrupt_close(), vec![next(10)]),
            (
                three_values(),
                vec![next(10), next(20), next(30), complete()],
            ),
        ],
    );
}

#[test]
fn map_err_conformance() {
    assert_matrix(
        "map_err",
        |stream| stream.map_err(|e| format!("mapped:{e}")),
        &[
            (no_event(), vec![]),
            (one_value(), vec![next(1), complete()]),
            (completion_only(), vec![complete()]),
            // Seule l'erreur **métier** est traduite.
            (business_failure(), vec![business("mapped:boom")]),
            (technical_failure(), vec![technical()]),
            // `Empty` ne porte pas de charge métier : il doit traverser.
            (empty_failure(), vec![empty()]),
            (abrupt_close(), vec![next(1)]),
            (three_values(), vec![next(1), next(2), next(3), complete()]),
        ],
    );
}

#[test]
fn scan_conformance() {
    assert_matrix(
        "scan",
        |stream| stream.scan(0, |acc, v| acc + v),
        &[
            (no_event(), vec![]),
            (one_value(), vec![next(1), complete()]),
            (completion_only(), vec![complete()]),
            (business_failure(), vec![business("boom")]),
            (technical_failure(), vec![technical()]),
            (empty_failure(), vec![empty()]),
            (abrupt_close(), vec![next(1)]),
            (three_values(), vec![next(1), next(3), next(6), complete()]),
        ],
    );
}

#[test]
fn tap_conformance() {
    assert_matrix(
        "tap",
        |stream| stream.tap(|_| {}),
        &[
            (no_event(), vec![]),
            (one_value(), vec![next(1), complete()]),
            (completion_only(), vec![complete()]),
            (business_failure(), vec![business("boom")]),
            (technical_failure(), vec![technical()]),
            (empty_failure(), vec![empty()]),
            (abrupt_close(), vec![next(1)]),
            (three_values(), vec![next(1), next(2), next(3), complete()]),
        ],
    );
}

#[test]
fn finalize_conformance() {
    assert_matrix(
        "finalize",
        |stream| stream.finalize(|| {}),
        &[
            (no_event(), vec![]),
            (one_value(), vec![next(1), complete()]),
            (completion_only(), vec![complete()]),
            (business_failure(), vec![business("boom")]),
            (technical_failure(), vec![technical()]),
            (empty_failure(), vec![empty()]),
            (abrupt_close(), vec![next(1)]),
            (three_values(), vec![next(1), next(2), next(3), complete()]),
        ],
    );
}

#[test]
fn start_with_conformance() {
    assert_matrix(
        "start_with",
        |stream| stream.start_with(99),
        &[
            // Le préfixe est émis avant tout poll de la source : sur un flux
            // vide, il est tout ce qu'on voit.
            (no_event(), vec![next(99)]),
            (one_value(), vec![next(99), next(1), complete()]),
            (completion_only(), vec![next(99), complete()]),
            (business_failure(), vec![next(99), business("boom")]),
            (technical_failure(), vec![next(99), technical()]),
            (empty_failure(), vec![next(99), empty()]),
            (abrupt_close(), vec![next(99), next(1)]),
            (
                three_values(),
                vec![next(99), next(1), next(2), next(3), complete()],
            ),
        ],
    );
}

#[test]
fn filter_conformance() {
    assert_matrix(
        "filter",
        |stream| stream.filter(|v| v % 2 == 1),
        &[
            (no_event(), vec![]),
            (one_value(), vec![next(1), complete()]),
            (completion_only(), vec![complete()]),
            (business_failure(), vec![business("boom")]),
            (technical_failure(), vec![technical()]),
            (empty_failure(), vec![empty()]),
            (abrupt_close(), vec![next(1)]),
            (three_values(), vec![next(1), next(3), complete()]),
        ],
    );
}

#[test]
fn take_conformance() {
    assert_matrix(
        "take",
        |stream| stream.take(2),
        &[
            (no_event(), vec![]),
            (one_value(), vec![next(1), complete()]),
            (completion_only(), vec![complete()]),
            (business_failure(), vec![business("boom")]),
            (technical_failure(), vec![technical()]),
            (empty_failure(), vec![empty()]),
            (abrupt_close(), vec![next(1)]),
            // La troisième valeur déclenche le `Complete` synthétisé : elle
            // n'est **pas** émise.
            (three_values(), vec![next(1), next(2), complete()]),
        ],
    );
}

#[test]
fn skip_conformance() {
    assert_matrix(
        "skip",
        |stream| stream.skip(1),
        &[
            (no_event(), vec![]),
            (one_value(), vec![complete()]),
            (completion_only(), vec![complete()]),
            (business_failure(), vec![business("boom")]),
            (technical_failure(), vec![technical()]),
            (empty_failure(), vec![empty()]),
            // La seule valeur est sautée, la fermeture reste une fermeture.
            (abrupt_close(), vec![]),
            (three_values(), vec![next(2), next(3), complete()]),
        ],
    );
}

#[test]
fn first_conformance() {
    assert_matrix(
        "first",
        |stream| stream.first(),
        &[
            // Un flux vide ne synthétise **pas** de `Complete` : il n'y a rien
            // à compléter.
            (no_event(), vec![]),
            (one_value(), vec![next(1), complete()]),
            (completion_only(), vec![complete()]),
            (business_failure(), vec![business("boom")]),
            (technical_failure(), vec![technical()]),
            (empty_failure(), vec![empty()]),
            (abrupt_close(), vec![next(1), complete()]),
            (three_values(), vec![next(1), complete()]),
        ],
    );
}

#[test]
fn catch_error_conformance() {
    assert_matrix(
        "catch_error",
        |stream| stream.catch_error(|_| -1),
        &[
            (no_event(), vec![]),
            (one_value(), vec![next(1), complete()]),
            (completion_only(), vec![complete()]),
            // La seule erreur rattrapée : la valeur de repli, puis la fin.
            (business_failure(), vec![next(-1), complete()]),
            // Un échec technique n'est pas rattrapable : il termine le flux.
            (technical_failure(), vec![technical()]),
            (empty_failure(), vec![empty()]),
            (abrupt_close(), vec![next(1)]),
            (three_values(), vec![next(1), next(2), next(3), complete()]),
        ],
    );
}

#[test]
fn take_until_conformance() {
    // Un token jamais annulé : le cas annulé est couvert par `super::tests`.
    let token = crate::CancellationToken::new();
    assert_matrix(
        "take_until",
        move |stream| stream.take_until(&token),
        &[
            (no_event(), vec![]),
            (one_value(), vec![next(1), complete()]),
            (completion_only(), vec![complete()]),
            (business_failure(), vec![business("boom")]),
            (technical_failure(), vec![technical()]),
            (empty_failure(), vec![empty()]),
            (abrupt_close(), vec![next(1)]),
            (three_values(), vec![next(1), next(2), next(3), complete()]),
        ],
    );
}
