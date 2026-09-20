use boaz_health_receiver::ack_journal::{AckJournal, AckReceipt, Baseline};
use sha2::{Digest, Sha256};
use std::fs;
use tempfile::TempDir;

fn adopted_journal(root: &std::path::Path) -> AckJournal {
    let journal = AckJournal::open(root).unwrap();
    let baseline = Baseline {
        snapshot_id: "synthetic-baseline".to_owned(),
        snapshot_sha256: "a".repeat(64),
        receipt_inventory_sha256: "b".repeat(64),
        control_store_id: "synthetic-control".to_owned(),
        control_head_sequence: 0,
        control_head_hash: "c".repeat(64),
    };
    journal.bind_baseline(&baseline).unwrap();
    assert_eq!(journal.baseline().unwrap().unwrap().baseline, baseline);
    journal
}

fn receipt(commit_sequence: i64) -> AckReceipt {
    AckReceipt {
        commit_sequence,
        received_at: "2026-09-19T00:00:00Z".to_owned(),
        accepted_events: 1,
        changed_events: 1,
        requires_projection: true,
    }
}

#[test]
fn durable_prepared_bytes_are_not_confirmed_until_receipt_commit() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().join("ack");
    fs::create_dir(&root).unwrap();
    AckJournal::initialize(&root).unwrap();
    let journal = adopted_journal(&root);
    let body = br#"{"batch_id":"b1","device_id":"d1"}"#;
    let prepared = journal.prepare_batch("b1", "d1", body).unwrap();
    assert_eq!(prepared.sequence, 1);
    assert_eq!(journal.raw_batch("b1").unwrap().unwrap(), body);
    assert!(
        !journal
            .is_confirmed("b1", "d1", &prepared.content_hash, 1)
            .unwrap()
    );
    journal.confirm_batch(&prepared, &receipt(9)).unwrap();
    let confirmed = journal.confirmed_batches().unwrap();
    assert_eq!(confirmed.len(), 1);
    assert_eq!(confirmed[0].prepared, prepared);
    assert_eq!(confirmed[0].receipt, receipt(9));
    assert_eq!(confirmed[0].raw, body);
    assert!(
        !journal
            .is_confirmed("b1", "d1", &prepared.content_hash, 1)
            .unwrap()
    );
    assert!(
        journal
            .is_confirmed("b1", "d1", &prepared.content_hash, 9)
            .unwrap()
    );
    assert!(
        journal
            .receipt_matches("b1", "d1", &prepared.content_hash, &receipt(9))
            .unwrap()
    );
    let mut forged_receipt = receipt(9);
    forged_receipt.changed_events = 0;
    assert!(
        !journal
            .receipt_matches("b1", "d1", &prepared.content_hash, &forged_receipt)
            .unwrap()
    );
    drop(journal);
    let reopened = AckJournal::open(&root).unwrap();
    assert_eq!(reopened.raw_batch("b1").unwrap().unwrap(), body);
    assert!(
        reopened
            .is_confirmed("b1", "d1", &prepared.content_hash, 9)
            .unwrap()
    );
}

#[test]
fn retries_preserve_bytes_and_sequence_and_conflicts_are_rejected() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().join("ack");
    fs::create_dir(&root).unwrap();
    AckJournal::initialize(&root).unwrap();
    let journal = adopted_journal(&root);
    let first = journal.prepare_batch("b1", "d1", b"exact body").unwrap();
    let retry = journal.prepare_batch("b1", "d1", b"exact body").unwrap();
    assert_eq!(retry.sequence, first.sequence);
    assert_eq!(retry.content_hash, first.content_hash);
    assert!(journal.prepare_batch("b1", "d1", b"different").is_err());
    assert!(journal.prepare_batch("b1", "d2", b"exact body").is_err());
    let next = journal.prepare_batch("b2", "d1", b"next").unwrap();
    assert_eq!(next.sequence, first.sequence + 1);
}

#[test]
fn missing_or_tampered_prepared_record_fails_closed() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().join("ack");
    fs::create_dir(&root).unwrap();
    AckJournal::initialize(&root).unwrap();
    let journal = adopted_journal(&root);
    let prepared = journal.prepare_batch("b1", "d1", b"body").unwrap();
    let path = root.join(format!("{:020}.prepared.json", prepared.sequence));
    fs::write(&path, b"tampered").unwrap();
    assert!(journal.confirm_batch(&prepared, &receipt(1)).is_err());
    assert!(AckJournal::open(&root).is_err());
    assert!(journal.raw_batch("b1").is_err());
}

#[test]
fn deleting_the_last_acknowledged_confirmation_is_detected() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().join("ack");
    fs::create_dir(&root).unwrap();
    AckJournal::initialize(&root).unwrap();
    let journal = adopted_journal(&root);
    let prepared = journal.prepare_batch("last", "phone", b"body").unwrap();
    journal.confirm_batch(&prepared, &receipt(7)).unwrap();
    fs::remove_file(root.join(format!("{:020}.confirmed.json", prepared.sequence))).unwrap();
    assert!(AckJournal::open(&root).is_err());
    assert!(
        journal
            .is_confirmed("last", "phone", &prepared.content_hash, 7)
            .is_err()
    );
}

#[test]
fn pairing_recovery_record_contains_hashes_but_no_plaintext_secret() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().join("ack");
    fs::create_dir(&root).unwrap();
    AckJournal::initialize(&root).unwrap();
    let journal = adopted_journal(&root);
    let code_hash = hex::encode(Sha256::digest(b"secret-code"));
    let token_hash = hex::encode(Sha256::digest(b"secret-token"));
    let pairing = journal
        .prepare_pairing("new-phone", &code_hash, &token_hash, "2026-09-19T00:00:00Z")
        .unwrap();
    journal.confirm_pairing(&pairing).unwrap();
    let recovery = journal.pairing_records().unwrap();
    assert_eq!(recovery.len(), 1);
    assert!(recovery[0].confirmed);
    assert_eq!(recovery[0].prepared, pairing);
    let bytes =
        fs::read(root.join(format!("pairing-{}.prepared.json", pairing.record_id))).unwrap();
    assert!(
        !bytes
            .windows(b"secret-code".len())
            .any(|part| part == b"secret-code")
    );
    assert!(
        !bytes
            .windows(b"secret-token".len())
            .any(|part| part == b"secret-token")
    );
    assert!(AckJournal::open(&root).is_ok());
    fs::remove_file(root.join(format!("pairing-{}.confirmed.json", pairing.record_id))).unwrap();
    assert!(AckJournal::open(&root).is_err());
}

#[test]
fn adoption_baseline_is_immutable_and_required_after_writes() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().join("ack");
    fs::create_dir(&root).unwrap();
    AckJournal::initialize(&root).unwrap();
    let journal = AckJournal::open(&root).unwrap();
    assert!(journal.prepare_batch("b1", "d1", b"body").is_err());
    let baseline = Baseline {
        snapshot_id: "synthetic-baseline".to_owned(),
        snapshot_sha256: "a".repeat(64),
        receipt_inventory_sha256: "b".repeat(64),
        control_store_id: "synthetic-control".to_owned(),
        control_head_sequence: 0,
        control_head_hash: "c".repeat(64),
    };
    journal.bind_baseline(&baseline).unwrap();
    let mut changed = baseline.clone();
    changed.snapshot_id = "other".to_owned();
    assert!(journal.bind_baseline(&changed).is_err());
    journal.prepare_batch("b1", "d1", b"body").unwrap();
    fs::remove_file(root.join("baseline.json")).unwrap();
    assert!(AckJournal::open(&root).is_err());
    assert!(journal.raw_batch("b1").is_err());
}

#[test]
fn external_checkpoint_detects_entire_journal_rollback() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().join("ack");
    fs::create_dir(&root).unwrap();
    AckJournal::initialize(&root).unwrap();
    let journal = adopted_journal(&root);
    let first = journal
        .prepare_batch("first", "phone", b"first body")
        .unwrap();
    journal.confirm_batch(&first, &receipt(1)).unwrap();
    let older_head = fs::read(root.join("head.json")).unwrap();
    let second = journal
        .prepare_batch("second", "phone", b"second body")
        .unwrap();
    journal.confirm_batch(&second, &receipt(2)).unwrap();
    let externally_sealed = journal.checkpoint().unwrap();
    drop(journal);
    fs::write(root.join("head.json"), older_head).unwrap();
    fs::remove_file(root.join(format!("{:020}.prepared.json", second.sequence))).unwrap();
    fs::remove_file(root.join(format!("{:020}.confirmed.json", second.sequence))).unwrap();
    let rolled_back = AckJournal::open(&root).unwrap();
    assert!(!rolled_back.contains_checkpoint(&externally_sealed).unwrap());
}

#[test]
fn baseline_bytes_are_sealed_beyond_syntactic_validation() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().join("ack");
    fs::create_dir(&root).unwrap();
    AckJournal::initialize(&root).unwrap();
    let journal = adopted_journal(&root);
    let sealed_sha = journal.baseline_sha256().unwrap().unwrap();
    drop(journal);
    let original = String::from_utf8(fs::read(root.join("baseline.json")).unwrap()).unwrap();
    let changed = original.replacen(&"a".repeat(64), &"f".repeat(64), 1);
    assert_ne!(changed, original);
    fs::write(root.join("baseline.json"), changed).unwrap();
    let replaced = AckJournal::open(&root).unwrap();
    assert_ne!(replaced.baseline_sha256().unwrap().unwrap(), sealed_sha);
}
