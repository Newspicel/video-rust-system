use crate::error::AppError;
use crate::jobs::{JobStage, LocalJobStore};
use uuid::Uuid;

#[tokio::test]
async fn local_job_store_lifecycle() -> Result<(), AppError> {
    let store = LocalJobStore::new();
    let id = Uuid::new_v4();

    store.create_job(id).await?;
    store
        .set_plan(id, vec![JobStage::Downloading, JobStage::Transcoding])
        .await?;

    let initial = store.status(&id).await?.expect("job missing after create");
    assert_eq!(initial.stage, JobStage::Queued);
    assert_eq!(initial.progress, 0.0);
    assert_eq!(initial.stage_progress, 0.0);
    assert_eq!(initial.current_stage_index, None);
    assert_eq!(initial.total_stages, 2);

    store.update_stage(id, JobStage::Downloading).await?;
    store.update_progress(id, 0.5).await?;

    let downloading = store.status(&id).await?.expect("job missing after update");
    assert_eq!(downloading.stage, JobStage::Downloading);
    assert!((downloading.stage_progress - 0.5).abs() < f32::EPSILON);
    assert!((downloading.progress - 0.25).abs() < f32::EPSILON);
    assert_eq!(downloading.current_stage_index, Some(1));
    assert_eq!(downloading.total_stages, 2);

    store.update_progress(id, 1.5).await?; // clamp to 1.0

    let downloading_complete = store
        .status(&id)
        .await?
        .expect("job missing after clamp");
    assert_eq!(downloading_complete.stage, JobStage::Downloading);
    assert!((downloading_complete.stage_progress - 1.0).abs() < f32::EPSILON);
    assert!((downloading_complete.progress - 0.5).abs() < f32::EPSILON);

    store.update_stage(id, JobStage::Transcoding).await?;
    store.update_progress(id, 0.4).await?;

    let transcoding = store.status(&id).await?.expect("job missing after stage change");
    assert_eq!(transcoding.stage, JobStage::Transcoding);
    assert!((transcoding.stage_progress - 0.4).abs() < f32::EPSILON);
    assert!((transcoding.progress - 0.7).abs() < f32::EPSILON);
    assert_eq!(transcoding.current_stage_index, Some(2));

    store.fail(id, "network".into()).await?;
    let failed = store.status(&id).await?.expect("job missing after fail");
    assert_eq!(failed.stage, JobStage::Failed);
    assert_eq!(failed.error.as_deref(), Some("network"));
    assert!((failed.progress - failed.stage_progress).abs() < f32::EPSILON);

    store.complete(id).await?;
    let complete = store
        .status(&id)
        .await?
        .expect("job missing after complete");
    assert_eq!(complete.stage, JobStage::Complete);
    assert_eq!(complete.progress, 1.0);
    assert!(complete.error.as_deref().is_some());
    assert!((complete.stage_progress - 1.0).abs() < f32::EPSILON);

    let all = store.list().await?;
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].id, id);

    Ok(())
}
