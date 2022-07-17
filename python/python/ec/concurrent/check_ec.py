#!/usr/bin/python

import commands
import sys
import threading


class RunEcShell(threading.Thread):
    def run(self):
        (status, output) = commands.getstatusoutput("hive --orcfiledump  hdfs://bipcluster03/bip/hive_warehouse/temp_nbi.db/mc_uv_access_scene_path_dt/dt=20200820 ")
        print ("output:"+output)
        print ("status:"+str(status))

if __name__ == '__main__':
    threads = 10
    runThreads = []
    for i in range(0, int(threads), 1):
        try:
            task = RunEcShell()
            task.start()
            runThreads.append(task)
        except:
            raise
    for thread in runThreads:
        thread.join()