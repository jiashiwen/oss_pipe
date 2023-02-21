use reqwest::Client;
use lazy_static::lazy_static;

lazy_static! {
   pub static ref GLOBAL_HTTP_CLIENT: Client = Client::new();
}

