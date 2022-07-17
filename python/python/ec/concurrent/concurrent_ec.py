import commands
import sys
import threading


class RunEcShell(threading.Thread):
    def __init__(self, times):
        threading.Thread.__init__(self)
        self.times = times

    def run(self):
        actionId = sys.argv[0]
        scheduleId = sys.argv[1]
        ecListTable = sys.argv[2]
        yarnQueue = sys.argv[3]
        (status, output) = commands.getstatusoutput("sh /home/vipshop/hive_etl_home/Test/tools/cloddata_transform_ec.sh "
                                 + actionId + " "
                                 + scheduleId + " "
                                 + ecListTable + " "
                                 + yarnQueue)

def run():
    threads = sys.argv[4]
    runThreads = []
    for i in range(0, int(threads), 1):
        try:
            task = RunEcShell(i)
            task.start()
            runThreads.append(task)
        except:
            raise
    for thread in runThreads:
        thread.join()

if __name__ == '__main__':
    run
