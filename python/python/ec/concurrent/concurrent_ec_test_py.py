import commands
import sys
import threading


class RunEcShell(threading.Thread):
    def run(self):

        #(status, output) = commands.getstatusoutput("ls -a ")
        print "haha"


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
