State是相当于可以在executor端修改的广播变量吗？
当并行度>1时，一个operator的多个operator sutask 分布在不同的物理机
同时修改State有问题吗？
state不是全局的吗？

https://www.cnblogs.com/xueqiuqiu/articles/13123955.html
https://blog.csdn.net/qq_31866793/article/details/97272103

state的容错，exactly once