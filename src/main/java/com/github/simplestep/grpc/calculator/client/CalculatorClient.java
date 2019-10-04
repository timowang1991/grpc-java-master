package com.github.simplestep.grpc.calculator.client;

import com.proto.calculator.CalculatorServiceGrpc;
import com.proto.calculator.PrimeNumberDecompositionRequest;
import com.proto.calculator.SumRequest;
import com.proto.calculator.SumResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class CalculatorClient {
    public static void main(String[] args) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50052)
                .usePlaintext()
                .build();

        CalculatorServiceGrpc.CalculatorServiceBlockingStub stub = CalculatorServiceGrpc.newBlockingStub(channel);

        // Unary
//        SumRequest sumRequest = SumRequest.newBuilder()
//                .setFirstNumber(10)
//                .setSecondNumber(25)
//                .build();
//
//        SumResponse sumResponse = stub.sum(sumRequest);
//        System.out.println(sumRequest.getFirstNumber() + " + " + sumRequest.getSecondNumber() + " = " + sumResponse.getSumResult());

        // Streaming Server
        Long number = 5694634545645654670L;
        stub.primeNumberDecomposition(PrimeNumberDecompositionRequest.newBuilder()
                .setNumber(number)
                .build()).forEachRemaining(primeNumberDecompositionResponse -> {
                    System.out.println("prime factors : " + primeNumberDecompositionResponse.getPrimeFactor());
        });


        channel.shutdown();
    }
}
