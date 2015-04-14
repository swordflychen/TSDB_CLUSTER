

all:
	@echo "Explicit target required." 
	@echo "Build benchmark test:"
	@echo "    make benchmark"
	@echo "Build regression test:"
	@echo "    make regression"

test: benchmark regression

benchmark:
	$(MAKE) -C ./benchmark
	cp ./benchmark/bench_test .

regression:
	cp ./regression/xxx ./reg_test.sh
	

clean:
	$(MAKE) -C ./benchmark clean
	$(MAKE) -C ./api/hiredis clean
	rm -rf ./bench_test
	rm -rf ./reg_test.sh


.PHONY: all benchmark regression clean
