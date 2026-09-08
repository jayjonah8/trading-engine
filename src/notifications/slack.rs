#![allow(dead_code)]

use slack_morphism::prelude::*;
use std::env;

// Slack setup guide: https://www.youtube.com/watch?v=h94FK8h1OJU

pub async fn send_slack_message(channel_id: &str, message: &str) {
    // Missing token = configuration error, so panic.
    let token = env::var("SLACK_TOKEN")
        .expect("Missing SLACK_TOKEN");

    // Slack/network failure = don't crash the bot.
    let Ok(connector) = SlackClientHyperConnector::new() else {
        eprintln!("SLACK ERROR: Failed to create connector");
        return;
    };

    let client = SlackClient::new(connector);

    let token = SlackApiToken::new(token.into());
    let session = client.open_session(&token);

    let request = SlackApiChatPostMessageRequest::new(
        channel_id.into(),
        SlackMessageContent::new()
            .with_text(message.into()),
    );

    if let Err(e) = session.chat_post_message(&request).await {
        eprintln!("SLACK ERROR: Failed to send message: {e}");
    }
}