.PHONY: test bench cluster clean

test:
	go test ./... -race -count=1

bench:
	go test ./... -race -bench=. -benchmem -short

cluster:
	@trap 'kill 0' EXIT; \
	go run main.go -id node1 -port 8001 & \
	sleep 0.5; \
	go run main.go -id node2 -port 8002 -peer 127.0.0.1:8001 & \
	sleep 0.5; \
	go run main.go -id node3 -port 8003 -peer 127.0.0.1:8002 & \
	wait

clean:
	go clean -testcache
