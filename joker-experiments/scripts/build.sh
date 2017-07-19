#!/bin/bash

ls marker >> /dev/null 2>&1

if [ $? != '0' ]; then
    echo "run the build script in scripts directory..."
    exit 1
fi

rm viz.py >> /dev/null 2>&1

rm joker-experiments-0.1.jar >> /dev/null 2>&1

cd ../..

if [ $? != '0' ]; then
    exit 1
fi

pwd

mvn clean package -DskipTests=true

if [ $? != '0' ]; then
    echo "run the build script in scripts directory..."
    exit 1
fi

cp joker-experiments/target/joker-experiments-0.1.jar joker-experiments/scripts/

if [ $? != '0' ]; then
    echo "run the build script in scripts directory..."
    exit 1
fi

cp joker-engine/viz.py joker-experiments/scripts/

if [ $? != '0' ]; then
    echo "run the build script in scripts directory..."
    exit 1
fi

cd joker-experiments/scripts

if [ $? != '0' ]; then
    echo "run the build script in scripts directory..."
    exit 1
fi

mkdir jokerwd

if [ $? != '0' ]; then
    echo "run the build script in scripts directory..."
    exit 1
fi

cp joker-experiments-0.1.jar jokerwd/

if [ $? != '0' ]; then
    echo "run the build script in scripts directory..."
    exit 1
fi

cp viz.py jokerwd/

if [ $? != '0' ]; then
    echo "run the build script in scripts directory..."
    exit 1
fi

cp ./*.sh jokerwd/

if [ $? != '0' ]; then
    echo "run the build script in scripts directory..."
    exit 1
fi

zip -r jokerwd.zip jokerwd/

if [ $? != '0' ]; then
    echo "run the build script in scripts directory..."
    exit 1
fi

rm -rf jokerwd/

if [ $? != '0' ]; then
    echo "run the build script in scripts directory..."
    exit 1
fi

echo "Done!"





