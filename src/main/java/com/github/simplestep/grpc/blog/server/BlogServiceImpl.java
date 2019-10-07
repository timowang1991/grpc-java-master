package com.github.simplestep.grpc.blog.server;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.proto.blog.Blog;
import com.proto.blog.BlogServiceGrpc;
import com.proto.blog.CreateBlogRequest;
import com.proto.blog.CreateBlogResponse;
import com.proto.blog.ReadBlogRequest;
import com.proto.blog.ReadBlogResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.bson.Document;
import org.bson.types.ObjectId;

public class BlogServiceImpl extends BlogServiceGrpc.BlogServiceImplBase {

    private MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
    private MongoDatabase database = mongoClient.getDatabase("mydb");
    private MongoCollection<Document> collection = database.getCollection("blog");

    @Override
    public void createBlog(CreateBlogRequest request, StreamObserver<CreateBlogResponse> responseObserver) {
        System.out.println("Received Create Blog request");

        Blog blog = request.getBlog();

        Document doc = new Document("author_id", blog.getAuthorId())
                .append("title", blog.getTitle())
                .append("content", blog.getContent());

        System.out.println("Inserting blog...");
        // we insert (create) the document in mongoDB
        collection.insertOne(doc);

        String id = doc.getObjectId("_id").toString();
        System.out.println("Inserted blog: " + id);

        CreateBlogResponse response = CreateBlogResponse.newBuilder()
                .setBlog(blog.toBuilder().setId(id).build()).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void readBlog(ReadBlogRequest request, StreamObserver<ReadBlogResponse> responseObserver) {
        System.out.println("Received Read Blog request");

        String blogId = request.getBlogId();

        System.out.println("Searching for a blog");

        Document result;
        try {
            result = collection.find(Filters.eq("_id", new ObjectId(blogId))).first();
        } catch (Exception e) {
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription("The blog with the corresponding id was not found")
                    .augmentDescription(e.getLocalizedMessage())
                    .asRuntimeException()
            );
            return;
        }

        if (result == null) {
            System.out.println("Blog not found");
            // we don't have a match
            responseObserver.onError(
                    Status.NOT_FOUND.withDescription("The blog with the corresponding id was not found")
                    .asRuntimeException()
            );
        } else {
            System.out.println("Blog found, sending response");

            Blog blog = Blog.newBuilder()
                    .setAuthorId(result.getString("author_id"))
                    .setTitle(result.getString("title"))
                    .setContent(result.getString("content"))
                    .setId(result.get("_id").toString())
                    .build();

            ReadBlogResponse response = ReadBlogResponse.newBuilder().setBlog(blog).build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}
