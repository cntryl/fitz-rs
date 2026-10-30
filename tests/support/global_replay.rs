use cntryl_fitz::client_domains::stream::{
    StreamCommitMode, StreamDiscriminator, StreamFilterClause, StreamFilterSet, StreamReadItem,
    StreamReadOptions,
};
use cntryl_fitz::{Client, Result};

pub(super) async fn exercise(connected: &Client, first_route: &str) -> Result<Vec<String>> {
    let stream = connected.stream()?;
    let initial = stream
        .read_page("stream://**", 0, 1, StreamReadOptions::default())
        .await?;
    let mut next = initial
        .cursor
        .captured_watermark
        .expect("global read returns a captured watermark");
    let second_route = first_route.replacen("stream://test-realm/", "stream://other-realm/", 1);
    let discriminator = StreamDiscriminator::from(format!("match-{first_route}"));
    let skipped = StreamDiscriminator::from("skip");
    let filter = StreamFilterSet {
        clauses: vec![StreamFilterClause::Equals(
            discriminator.as_str().to_string(),
        )],
    };
    for route in [first_route, second_route.as_str()] {
        let mut session = stream.begin(route, None).await?;
        session.append(0, b"skip", None, Some(&skipped)).await?;
        session
            .append(1, b"keep", None, Some(&discriminator))
            .await?;
        session.commit(StreamCommitMode::Sync).await?;
    }

    let mut records = Vec::new();
    let mut fingerprint = None;
    let mut watermark = None;
    let mut filtered_only = false;
    let mut pages = 0;
    loop {
        let page = stream
            .read_page(
                "stream://**",
                next,
                1,
                StreamReadOptions {
                    filter: Some(&filter),
                    cursor_fingerprint: fingerprint,
                    captured_watermark: watermark,
                    ..StreamReadOptions::default()
                },
            )
            .await?;
        pages += 1;
        let mut page_events = 0;
        let had_items = !page.items.is_empty();
        for item in page.items {
            if let StreamReadItem::Event(record) = item {
                page_events += 1;
                records.push(record);
            }
        }
        filtered_only |= had_items && page_events == 0;
        if !page.cursor.has_more {
            break;
        }
        let last = page
            .cursor
            .last_global_offset
            .expect("global cursor offset");
        assert!(last >= next, "global cursor must advance");
        next = last + 1;
        fingerprint = page.cursor.cursor_fingerprint;
        watermark = page.cursor.captured_watermark;
    }

    assert!(pages > 1 && filtered_only);
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].route, first_route);
    assert_eq!(records[1].route, second_route);
    let first = records[0].global_offset.expect("first global offset");
    let second = records[1].global_offset.expect("second global offset");
    assert!(first < second);
    Ok(vec![
        format!("global filtered replay yielded {pages} pages across two realms"),
        "filtered-only pages advanced the global cursor through the captured tail".to_string(),
    ])
}
