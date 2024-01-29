# See https://hub.docker.com/r/ibmcom/mq
# https://developer.ibm.com/tutorials/mq-connect-app-queue-manager-containers/
pull:
	#docker pull icr.io/ibm-messaging/mq:9.3.2.0-r2
	docker pull icr.io/ibm-messaging/mq:latest

create_volume:
	docker volume create qm1data

run:
	docker run --env LICENSE=accept \
		--env MQ_QMGR_NAME=QM1 \
		--volume qm1data:/mnt/mqm \
		--publish 1414:1414 \
		--publish 9443:9443 \
		--detach \
		--env MQ_APP_PASSWORD=passw0rd \
		--name QM1 icr.io/ibm-messaging/mq:latest

attach:
	docker exec -it QM1 bash
# We can then create a qlocal using define qlocal(Q1) maxdepth(100000)
# We can clear a queue using clear qlocal(Q1)
# We can see the queue depth using display qlocal(Q1) curdepth
# We can set the maximum queue deph using alter qlocal(Q1) maxdepth(100000)