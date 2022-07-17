from xml.dom.minidom import parse
import xml.dom.minidom
import xml
import sys

def getQueue(parent, queue_name):
    for q in parent.getElementsByTagName("queue"):
        nodes = q.getElementsByTagName("queueName")
        for node in nodes:
            if node.childNodes[0].data == queue_name:
                return q

def printResource(queue_name, node_label=''):
    q = collection
    for part in queue_name.split('.'):
        q = getQueue(q, part)

    resources = list(filter(lambda e: e.nodeName == "resources", q.childNodes))[0]
    for e in resources.childNodes:
        if e.getElementsByTagName("partitionName")[0].childNodes == [] and node_label == '' or e.getElementsByTagName("partitionName")[0].childNodes != [] and e.getElementsByTagName("partitionName")[0].childNodes[0].data == node_label:
            pending = e.getElementsByTagName("pending")[0]
            # Queue PendingMB
            print(pending.getElementsByTagName("memory")[0].childNodes[0].data)
            # Queue PendingVcore
            print(pending.getElementsByTagName("vCores")[0].childNodes[0].data)

            used = e.getElementsByTagName("used")[0]
            # Queue Memory Usage
            print(used.getElementsByTagName("memory")[0].childNodes[0].data)
            # Queue Vcores Usage
            print(used.getElementsByTagName("vCores")[0].childNodes[0].data)
            break

    # Queue Pending Containers
    print(q.getElementsByTagName("pendingContainers")[0].childNodes[0].data)
    # Queue Pending Apps
    print(q.getElementsByTagName("numPendingApplications")[0].childNodes[0].data)

if __name__ == '__main__':
    #printResource('isolate_kepler.magic_platform', 'isolate_kepler')
    DOMTree = xml.dom.minidom.parse(sys.argv[3])
    collection = DOMTree.documentElement
    printResource(sys.argv[1], sys.argv[2])