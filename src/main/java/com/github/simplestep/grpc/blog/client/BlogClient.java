package com.github.simplestep.grpc.blog.client;

import com.proto.blog.Blog;
import com.proto.blog.BlogServiceGrpc;
import com.proto.blog.CreateBlogRequest;
import com.proto.blog.CreateBlogResponse;
import com.proto.blog.ReadBlogRequest;
import com.proto.blog.ReadBlogResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class BlogClient {
    public static void main(String[] args) {
        BlogClient main = new BlogClient();
        main.run();
    }

    public void run() {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50051)
                .usePlaintext()
                .build();

        createBlog(channel);
        readBlog(channel);
        readBlogNotFound(channel);

        System.out.println("client channel shutdown");

        channel.shutdown();
    }

    private void createBlog(ManagedChannel channel) {
        BlogServiceGrpc.BlogServiceBlockingStub blogClient = BlogServiceGrpc.newBlockingStub(channel);

        Blog blog = Blog.newBuilder()
                .setAuthorId("Stephane")
                .setTitle("New blog!")
                .setContent("Hello world this is my first blog!")
                .build();

        CreateBlogResponse response = blogClient.createBlog(CreateBlogRequest.newBuilder()
                .setBlog(blog).build());

        System.out.println("Received create blog response");
        System.out.println(response.toString());
    }

    private void readBlog(ManagedChannel channel) {
        System.out.println("Reading blog...");
        BlogServiceGrpc.BlogServiceBlockingStub blogClient = BlogServiceGrpc.newBlockingStub(channel);

        ReadBlogResponse response = blogClient.readBlog(ReadBlogRequest.newBuilder()
                .setBlogId("5d9b35d95fe3c17017febcca")
                .build());

        System.out.println(response.toString());
    }

    private void readBlogNotFound(ManagedChannel channel) {
        System.out.println("Reading blog with non existing id...");
        BlogServiceGrpc.BlogServiceBlockingStub blogClient = BlogServiceGrpc.newBlockingStub(channel);

        ReadBlogResponse response = blogClient.readBlog(ReadBlogRequest.newBuilder()
                .setBlogId("5d9b35d95fe3c17017febccb")
                .build());

        System.out.println(response.toString());
    }
}