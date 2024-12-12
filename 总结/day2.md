## 数仓开发之DIM层

#### 1.DIM层设计要点

```
1）DIM层的设计依据是维度建模理论，该层存储维度模型的维度表。

2）DIM层的数据存储在 HBase 表中

DIM 层表是用于维度关联的，要通过主键去获取相关维度信息，这种场景下 K-V 类型数据库的效率较高。常见的 K-V 类型数据库有 Redis、HBase，而 Redis 的数据常驻内存，会给内存造成较大压力，因而选用 HBase 存储维度数据。
3）DIM层表名的命名规范为dim_表名
```

#### 2.App基类设计

```
2.1 模板方法设计模式

1）定义

在父类中定义完成某一个功能的核心算法骨架，具体的实现可以延迟到子类中完成。模板方法类一定是抽象类，里面有一套具体的实现流程（可以是抽象方法也可以是普通方法）。这些方法可能由上层模板继承而来。

2）优点

在不改变父类核心算法骨架的前提下，每一个子类都可以有不同的实现。我们只需要关注具体方法的实现逻辑而不必在实现流程上分心。

2.2 基类设计思路

Flink Job的处理流程大致可以分为以下几步：

（1）初始化流处理环境，配置检查点，从Kafka中读取目标主题数据

（2）执行核心处理逻辑

（3）执行

其中，所有Job的第一步和第三步基本相同，我们可以定义基类，将这两步交给基类完成。定义抽象方法，用于实现核心处理逻辑，子类只需要重写该方法即可实现功能。省去了大量的重复代码，且不必关心整体的处理流程。

2.3 代码实现

1）创建Constant常量类

本项目会用到大量的配置信息，如主题名、Kafka集群地址等，为了减少冗余代码，保证一致性，将这些配置信息统一抽取到常量类中。

在com.hadoop.gmall.realtime.common模块的包下创建常量类Constant
```



![实时数仓dim的流程](C:/Users/LENOVO/Desktop/实时数仓dim的流程.png)

#### 数据倾斜问题

```
现有1亿条数据，求每个地区的数据量，两个地区一个为8千万，一个为八百万，发生了严重的倾斜
一般倾斜基本表现
Hive 中数据倾斜的基本表现一般都发生在 Sql 中 group by 和 join on 上，而且和数据逻辑绑定比较深。
key的分布不均匀或者说某些key太集中
业务数据自身的特性，例如不同数据类型关联产生数据倾斜


解决数据倾斜优化的一种方法
with t1 as (
    select user_id,
           order_no,
           concat(region,'-',rand()) as  region,
           product_no,
           color_no,
           sale_amount,
           ts
    from data_incline_t
)
select substr(region,1,2) as region,
       count(*) as cnt
from (
    select region,
           count(*) as cnt
    from t1
    group by region
     ) as t2
group by substr(region,1,2);

hive调优：
方法一：
上面方法，首先把地区进行拼接一个随机数，目的是为了把数据打散，然后进行分组查个数，这时因为拼接了随机数，数量大概率不会重复，
在套用一层子查询，把地区截取出来，再次进行一次聚合这时就会减少运行的时间
方法二：
增大map的个数方法
set mapred.map.tasks=20;
set mapred.min.split.size=100000000;
```
#### 12.12 遇到的问题
```
CentOs7安装redis时报错
因为redis6版本及以上需要gcc版本为9以上

yum -y install centos-release-scl  安装scl工具
如果你使用了 devtoolset（例如 devtoolset-9）来安装较新版本的 GCC，必须通过 scl enable 命令来启用它。

确保你在终端中执行了以下命令：

不可以直接安装，必须设置--nogpgcheck参数
yum install --nogpgcheck devtoolset-9   免除GPS秘钥的问题
使用 scl 启用 devtoolset：
scl enable devtoolset-9 bash   启用devtoolset-9
需要重新启动会话，版本gcc版本就会变为9及以上


yum不能正常工作
第一次尝试 换源
1：备份之前的yum源配置
mv /etc/yum.repos.d/CentOS-Base.repo /etc/yum.repos.d/CentOS-Base.repo.backup
2：下载阿里云的Yum源配置使用
curl -o /etc/yum.repos.d/CentOS-Base.repo http://mirrors.aliyun.com/repo/Centos-7.repo
3：清除缓存
yum clean all
4：加载缓存
yum makecache

第二次尝试 修改其他配置
修改CentOS-SCLo-scl-rh 文件，不存在不管
vi /etc/yum.repos.d/CentOS-SCLo-scl-rh.repo
替换内容：这里不是全部替换，只替换【centos-sclo-rh】下面的内容。

[centos-sclo-rh]
name=CentOS-$releasever - SCLo rh
baseurl=https://mirrors.aliyun.com/centos/$releasever/sclo/$basearch/rh/
gpgcheck=1
enabled=1
gpgkey=https://mirrors.aliyun.com/centos/RPM-GPG-KEY-CentOS-7
修改CentOS-SCLo-scl 文件，不存在不管
vi /etc/yum.repos.d/CentOS-SCLo-scl.repo
替换内容：这里不是全部替换，只替换【centos-sclo-sclo】下面的内容。

[centos-sclo-sclo]
name=CentOS-$releasever - SCLo sclo
baseurl=https://mirrors.aliyun.com/centos/$releasever/sclo/$basearch/sclo/
gpgcheck=1
enabled=1
gpgkey=https://mirrors.aliyun.com/centos/RPM-GPG-KEY-CentOS-7
```


