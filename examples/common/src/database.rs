//! Business database service.
//!
//! Provides people lookup queries (by last name/first name, by age).
//! Depends on `ConfigService` for its connection configuration.

use ice_rpc::{service, timeout, Observable};
use rkyv::{Archive, Deserialize, Serialize};

/// Error returned by the [`DatabaseService`] operations.
#[derive(Debug, Archive, Deserialize, Serialize, serde::Serialize, serde::Deserialize)]
pub enum DatabaseError {
    /// No record found for the query.
    NotFound,
    /// Internal database error.
    Error,
}

impl std::fmt::Display for DatabaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DatabaseError::NotFound => write!(f, "record not found"),
            DatabaseError::Error => write!(f, "internal database error"),
        }
    }
}

/// Search criteria for a person by their identity.
#[derive(Debug, Clone, Archive, Deserialize, Serialize, serde::Serialize, serde::Deserialize)]
pub struct PersonneQuery {
    /// Last name of the searched person.
    pub nom: String,
    /// First name of the searched person.
    pub prenom: String,
}

/// Full information about a person.
#[derive(Debug, Clone, Archive, Deserialize, Serialize, serde::Serialize, serde::Deserialize)]
pub struct PersonneInfo {
    /// Last name.
    pub nom: String,
    /// First name.
    pub prenom: String,
    /// Age in years.
    pub age: u32,
    /// Email address.
    pub email: String,
    /// Phone number.
    pub telephone: String,
    /// City of residence.
    pub ville: String,
    /// Current occupation.
    pub profession: String,
}

/// Business query service over a database.
///
/// Depends on `ConfigService` to obtain the connection parameters
/// (connection string, credentials, etc.).
#[service("DatabaseService")]
pub trait DatabaseService {
    /// Returns the age associated with a person's name.
    ///
    /// # Arguments
    /// * `name` — Name of the searched person.
    ///
    /// # Returns
    /// * `Ok(stream)` emitting `Next(age)` then `Complete`.
    /// * `Err(DatabaseError::NotFound)` if the name is unknown.
    #[timeout("10s")]
    async fn get_user_age(&self, name: String) -> Observable<i32, DatabaseError>;

    /// Returns the full information of a person.
    ///
    /// # Arguments
    /// * `query` — Search criteria (last name and first name).
    ///
    /// # Returns
    /// * `Ok(stream)` emitting `Next(PersonneInfo)` then `Complete`.
    /// * `Err(DatabaseError::NotFound)` if the person is not found.
    /// * `Err(DatabaseError::Error)` on internal error.
    async fn get_person(&self, query: PersonneQuery) -> Observable<PersonneInfo, DatabaseError>;
}
