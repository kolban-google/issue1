PROJECT_ID=test1-305123
BUCKET_NAME=kolban-dataflow
REGION=us-central1
run:
	mvn -Pdataflow-runner compile exec:java \
        -Dexec.mainClass=com.kolban.beam.MQPipe \
        -Dexec.args="--project=$(PROJECT_ID) \
        --gcpTempLocation=gs://$(BUCKET_NAME)/temp/ \
        --runner=DataflowRunner \
        --maxNumWorkers=10 \
        --numWorkers=10 \
        --enableStreamingEngine=true \
        --autoscalingAlgorithm=THROUGHPUT_BASED \
        --region=$(REGION)"