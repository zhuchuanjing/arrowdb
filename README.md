**自己实现的基于 HNSW 的向量数据库**

为什么要自己实现 因为没有一个较好的 内存和 持续化设施 结合的 HNSW 实现, usearch 是 C++的 hnsw_lib 没有考虑持续化

基本原则 加入数据会获得一个唯一的 ID u64 应该足够了
注意 不能随便删除一个 point 因为涉及到整个导航网络 少量的数据更改 改变向量可以直接进行 导航网格可以自己调整过来

基本功能
就是一个单纯的 向量数据库 支持多个向量集合

#### collection -> 创建 集合 删除 集合 获取集合列表
#### 向集合增加一个向量 返回向量的 id（u64)
#### 批量增加向量
#### 更改 指定 id 的向量
#### 删除指定 id
#### 获取 集合向量的数量(包括已经删除的 向量)

### 下面是一个简单的示例程序
```
use anyhow::Result;
use arrowdb::db::ArrowDB;
use rand::distributions::Uniform;
use rand::prelude::*;

fn main() -> Result<()> {
    let arrow_db = ArrowDB::new("arrow_db");
    let dim = 1024;
    arrow_db.create_collection("test1", 128).unwrap();
    let a_db = arrow_db.clone();
    a_db.create_collection("test2", dim).unwrap();

    println!("{:?}", arrow_db.get_collections());
    let nb_elem = 1024;
    let mut rng = thread_rng();
    let unif = Uniform::<f32>::new(0., 1.);
    let mut data = Vec::with_capacity(nb_elem);
        for _ in 0..nb_elem {
        let column = (0..dim).map(|_| rng.sample(unif)).collect::<Vec<f32>>();
        data.push(column);
    }

    let data_with_id = data.iter().zip(0..data.len()).collect::<Vec<_>>();  // give an id to each data

    let (dim, hns) = arrow_db.get_hnsw("test2").unwrap();
    for _i in 0..data_with_id.len() {
        hns.insert(data_with_id[_i].0.clone());
    }
    println!("{:?}", hns.search(data_with_id[100].0.clone(), 10, 200));
    Ok(())
}
```
[arrowx provide a simple http server of arrowdb](https://github.com/zhuchuanjing/arrowx)
