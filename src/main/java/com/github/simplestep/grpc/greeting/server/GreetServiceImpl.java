package com.github.simplestep.grpc.greeting.server;

import com.proto.greet.GreetEveryoneRequest;
import com.proto.greet.GreetEveryoneResponse;
import com.proto.greet.GreetManyTimesRequest;
import com.proto.greet.GreetManyTimesResponse;
import com.proto.greet.GreetRequest;
import com.proto.greet.GreetResponse;
import com.proto.greet.GreetServiceGrpc;
import com.proto.greet.Greeting;
import com.proto.greet.LongGreetRequest;
import com.proto.greet.LongGreetResponse;
import io.grpc.stub.StreamObserver;

public class GreetServiceImpl extends GreetServiceGrpc.GreetServiceImplBase {

    @Override
    public void greet(GreetRequest request, StreamObserver<GreetResponse> responseObserver) {
        // extract the fields we need
        Greeting greeting = request.getGreeting();
        String firstName = greeting.getFirstName();

        // create the response
        String result = "Hello " + firstName;
        GreetResponse response = GreetResponse.newBuilder()
                .setResult(result)
                .build();

        // send the response
        responseObserver.onNext(response);

        // complete the RPC call
        responseObserver.onCompleted();
    }

    @Override
    public void greetManyTimes(GreetManyTimesRequest request, StreamObserver<GreetManyTimesResponse> responseObserver) {
        String firstName = request.getGreeting().getFirstName();

        try {
            for (int i = 0; i < 10; i++) {
                String result = "Hello " + firstName + ", response number: " + i;
                GreetManyTimesResponse greetManyTimesResponse = GreetManyTimesResponse.newBuilder()
                        .setResult(result)
                        .build();

                responseObserver.onNext(greetManyTimesResponse);
                Thread.sleep(1000L);
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            responseObserver.onCompleted();
        }
    }

    @Override
    public StreamObserver<LongGreetRequest> longGreet(StreamObserver<LongGreetResponse> responseObserver) {
        StreamObserver<LongGreetRequest> requestObserver = new StreamObserver<LongGreetRequest>() {

            String result = "";

            @Override
            public void onNext(LongGreetRequest value) {
                // client sends a message
                result += "Hello " + value.getGreeting().getFirstName() + "! ";
            }

            @Override
            public void onError(Throwable t) {
                // client sends an error
            }

            @Override
            public void onCompleted() {
                // client is done
                responseObserver.onNext(
                        LongGreetResponse.newBuilder()
                                .setResult(result)
                                .build()
                );
                responseObserver.onCompleted();
                // this is when we want to return a response (responseobserver)

            }
        };

        return requestObserver;
    }

    @Override
    public StreamObserver<GreetEveryoneRequest> greetEveryone(StreamObserver<GreetEveryoneResponse> responseObserver) {
        StreamObserver<GreetEveryoneRequest> requestObserver = new StreamObserver<GreetEveryoneRequest>() {
            @Override
            public void onNext(GreetEveryoneRequest value) {
                String response = "Hello " + value.getGreeting().getFirstName();
                responseObserver.onNext(GreetEveryoneResponse.newBuilder()
                        .setResult(response).build());
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        };

        return requestObserver;
    }
}
