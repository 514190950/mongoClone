package com.tincery.mongoclone;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClients;
import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDbFactory;
import org.springframework.util.StringUtils;

import java.util.Collections;
import java.util.List;


/**
 * @author gxz gongxuanzhang@foxmail.com
 **/
public class Main extends Application {


    Button start = new Button("开始复制");
    Label form = new Label("form");
    Label to = new Label("to");
    TextField newHost = new TextField("地址");
    TextField newPort = new TextField("端口");
    TextField oldUsername = new TextField("用户名");
    TextField oldPassword = new TextField("密码");
    TextField newUsername = new TextField("用户名");
    TextField newPassword = new TextField("密码");
    TextField database = new TextField("数据库名称，多个数据库用\",\"分割，form和to必须有相同名称的数据库名");
    TextField oldHost = new TextField("地址");
    TextField oldPort = new TextField("端口");
    TextField limit = new TextField("要复制的数量(如果想全复制写-1)");

    {
        start.setOnAction((event) -> {
            String db = this.database.getText();
            int scanCount = -1;
            if(!StringUtils.isEmpty(limit.getText())){
                scanCount = Integer.parseInt(limit.getText());
            }
            if (db.contains(",")) {
                for (String dbString : db.split(",")) {
                    MongoTemplate oldMongo = getMongoTemplate(this.oldHost.getText(),
                            Integer.parseInt(this.oldPort.getText()),
                            dbString, this.oldUsername.getText(), this.oldPassword.getText());

                    MongoTemplate newMongo = getMongoTemplate(this.newHost.getText(),
                            Integer.parseInt(this.newPort.getText()), dbString, this.newUsername.getText(),
                            this.newPassword.getText());
                    new MongoGetter(oldMongo,newMongo,scanCount).mongoClone();

                }
            } else {
                MongoTemplate oldMongo = getMongoTemplate(this.oldHost.getText(),
                        Integer.parseInt(this.oldPort.getText()),
                        db, this.oldUsername.getText(), this.oldPassword.getText());

                MongoTemplate newMongo = getMongoTemplate(this.newHost.getText(),
                        Integer.parseInt(this.newPort.getText()), db, this.newUsername.getText(),
                        this.newPassword.getText());
                new MongoGetter(oldMongo,newMongo,scanCount).mongoClone();
            }
            Alert alert = new Alert(Alert.AlertType.INFORMATION);
            alert.titleProperty().set("成功");
            alert.headerTextProperty().set("完成了");
            alert.showAndWait();
        });
    }

    @Override
    public void start(Stage primaryStage) {
        primaryStage.setTitle("MongoClone");
        VBox vBox = new VBox();
        HBox hBox1 = new HBox();
        HBox hBox2 = new HBox();
        vBox.getChildren().addAll(hBox1, database, hBox2, limit, start);
        hBox1.getChildren().addAll(form, oldHost, oldPort, oldUsername, oldPassword);
        hBox2.getChildren().addAll(to, newHost, newPort, newUsername, newPassword);
        primaryStage.setScene(new Scene(vBox));

        primaryStage.show();

    }


    public static void main(String[] args) {
        launch(args);
    }

    private MongoTemplate getMongoTemplate(String host, int port, String db, String username, String password) {
        if (StringUtils.isEmpty(username)) {
            return new MongoTemplate(MongoClients.create("mongodb://" + host + ":" + port), db);
        } else {
            List<ServerAddress> serverAddresses = Collections.singletonList(new ServerAddress(host, port));
            MongoCredential credential = MongoCredential.createCredential(username, db, password.toCharArray());
            MongoClientSettings set = MongoClientSettings.builder().credential(credential)
                    .applyToClusterSettings(settings -> settings.hosts(serverAddresses)).build();
            return new MongoTemplate(new SimpleMongoClientDbFactory(MongoClients.create(set), db));
        }
    }
}

