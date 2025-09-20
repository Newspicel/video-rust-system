use crate::error::AppError;
use crate::jobs::{JobStage, LocalJobStore};
use uuid::Uuid;

#[tokio::test]
async fn local_job_store_lifecycle() -> Result<(), AppError> {
    let store = LocalJobStore::new();
    let id = Uuid::new_v4();

    store.create_job(id).await?;

    let initial = store.status(&id).await?.expect("job missing after create");
    assert_eq!(initial.stage, JobStage::Queued);
    assert_eq!(initial.progress, 0.0);

    store.update_stage(id, JobStage::Uploading).await?;
    store.update_progress(id, 1.5).await?; // should clamp to 1.0

    let uploading = store.status(&id).await?.expect("job missing after update");
    assert_eq!(uploading.stage, JobStage::Uploading);
    assert_eq!(uploading.progress, 1.0);

    store.fail(id, "network".into()).await?;
    let failed = store.status(&id).await?.expect("job missing after fail");
    assert_eq!(failed.stage, JobStage::Failed);
    assert_eq!(failed.error.as_deref(), Some("network"));

    store.complete(id).await?;
    let complete = store
        .status(&id)
        .await?
        .expect("job missing after complete");
    assert_eq!(complete.stage, JobStage::Complete);
    assert_eq!(complete.progress, 1.0);
    assert!(complete.error.is_some());

    let all = store.list().await?;
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].id, id);

    Ok(())
}
