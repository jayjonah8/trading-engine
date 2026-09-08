#![allow(dead_code)]

use serenity::{http::Http, model::id::ChannelId};
use std::env;

pub async fn send_discord_message(channel_id: u64, message: &str) {
    let token = env::var("DISCORD_TOKEN").expect("Missing DISCORD_TOKEN");
    let http = Http::new(&token);
    let _ = ChannelId(channel_id).say(&http, message).await;
}