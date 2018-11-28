The code used to generate the files used in the tests is located in /resources/memory_impact directory.

To reduce the amount of memory reserved to tests ran on local executors (except standalone), they should be executed with "-Xms900m -Xmx900m" JVM arguments. These
arguments should be set directly in `java ... my_jar.jar` comman    d - otherwise they won't take any effect.