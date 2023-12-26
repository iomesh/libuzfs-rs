use uzfs::*;

#[tokio::main]
async fn main() {
    uzfs_env_init().await;
    let ds = Dataset::init("testzp/ds", "/dev/sda", DatasetType::Data)
        .await
        .unwrap();
    let obj = ds.create_objects(1).await.unwrap().0[0];
    let obj_hdl = ds.get_object_handle(obj).await.unwrap();
    let buf = vec![1; 4096];
    for _ in 0..2 {
        ds.write_object(obj_hdl, 0, false, &buf).await.unwrap();
    }
    ds.delete_object(obj_hdl).await.unwrap();
    Dataset::destroy_object_handle(obj_hdl).await;
    ds.close().await.unwrap();
    uzfs_env_fini().await;
}
