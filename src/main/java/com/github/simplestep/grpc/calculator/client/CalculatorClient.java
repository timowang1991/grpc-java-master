package com.github.simplestep.grpc.calculator.client;

import com.proto.calculator.CalculatorServiceGrpc;
import com.proto.calculator.ComputeAverageRequest;
import com.proto.calculator.ComputeAverageResponse;
import com.proto.calculator.PrimeNumberDecompositionRequest;
import com.proto.calculator.SumRequest;
import com.proto.calculator.SumResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CountDownLatch;

public class CalculatorClient {
    public static void main(String[] args) {
        CalculatorClient main = new CalculatorClient();
        main.run();
    }

    public void run() {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50052)
                .usePlaintext()
                .build();

//        doUnaryCall(channel);
//        doServerStreamingCall(channel);
        doClientStreamCall(channel);

        System.out.println("client channel shutdown");

        channel.shutdown();
    }

    private void doUnaryCall(ManagedChannel channel) {
        CalculatorServiceGrpc.CalculatorServiceBlockingStub stub = CalculatorServiceGrpc.newBlockingStub(channel);

        SumRequest sumRequest = SumRequest.newBuilder()
                .setFirstNumber(10)
                .setSecondNumber(25)
                .build();

        SumResponse sumResponse = stub.sum(sumRequest);
        System.out.println(sumRequest.getFirstNumber() + " + " + sumRequest.getSecondNumber() + " = " + sumResponse.getSumResult());
    }

    private void doServerStreamingCall(ManagedChannel channel) {
        CalculatorServiceGrpc.CalculatorServiceBlockingStub stub = CalculatorServiceGrpc.newBlockingStub(channel);

        Long number = 5694634545645654670L;
        stub.primeNumberDecomposition(PrimeNumberDecompositionRequest.newBuilder()
                .setNumber(number)
                .build()).forEachRemaining(primeNumberDecompositionResponse -> {
            System.out.println("prime factors : " + primeNumberDecompositionResponse.getPrimeFactor());
        });
    }

    private void doClientStreamCall(ManagedChannel channel) {
        CalculatorServiceGrpc.CalculatorServiceStub asyncClient = CalculatorServiceGrpc.newStub(channel);

        CountDownLatch latch = new CountDownLatch(1);

        StreamObserver<ComputeAverageRequest> requestObserver = asyncClient.computeAverage(new StreamObserver<ComputeAverageResponse>() {
            @Override
            public void onNext(ComputeAverageResponse value) {
                System.out.println("Received a response from the server");
                System.out.println(value.getAverage());
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onCompleted() {
                System.out.println("Server has completed sending us data");
                latch.countDown();
            }
        });

        // we send 10000 messages to our server (client streaming)
        for (int i = 0; i < 10000; i++) {
            requestObserver.onNext(ComputeAverageRequest.newBuilder()
                    .setNumber(i).build());
        }

        requestObserver.onCompleted();
        try {
            latch.await();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
