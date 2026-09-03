use crust::media::{JsonObject, MediaTrack};
use serde_json::{Value, json};

/// One canonical Lavalink v4 track serializer for REST, player snapshots, and events.
pub(crate) fn track_value(track: &MediaTrack, position_ms: u64, user_data: &JsonObject) -> Value {
    let metadata = &track.metadata;
    json!({
        "encoded": track.encoded.as_str(),
        "info": {
            "identifier": metadata.identifier,
            "isSeekable": metadata.seekable,
            "author": metadata.author,
            "length": metadata.duration_ms,
            "isStream": metadata.stream,
            "position": position_ms,
            "title": metadata.title,
            "uri": metadata.uri,
            "sourceName": metadata.source_name,
            "artworkUrl": metadata.artwork_url,
            "isrc": metadata.isrc,
        },
        "pluginInfo": track.plugin_info,
        "userData": user_data,
    })
}

pub(crate) fn loaded_track_value(track: &MediaTrack) -> Value {
    track_value(track, 0, &JsonObject::new())
}
