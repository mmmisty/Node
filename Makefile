all:
	javac -d . src/com/company/*.java
	jar -cvfe Node.jar com.company.Main com/company/*
clean:
	rm -rf com/
