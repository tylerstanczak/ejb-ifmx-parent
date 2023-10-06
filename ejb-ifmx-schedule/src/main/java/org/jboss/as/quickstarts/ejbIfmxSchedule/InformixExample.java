/*
 * JBoss, Home of Professional Open Source
 * Copyright 2015, Red Hat, Inc. and/or its affiliates, and individual
 * contributors by the @authors tag. See the copyright.txt in the
 * distribution for a full listing of individual contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jboss.as.quickstarts.ejbIfmxSchedule;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;
import javax.ejb.Schedule;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.annotation.PostConstruct;
import org.apache.commons.lang3.RandomStringUtils;

/**
 * Demonstrates how to read and write data to an Informix DB every 6 seconds
 *
 * @author <a href="mailto:tyler.stanczak@ibm.com">Tyler Stanczak</a>
 * @author <a href="mailto:vivin.abraham@ibm.com">Vivin Abraham</a>
 */
@Startup
@Singleton
public class InformixExample {
    Connection conn = null;
    String selectQuery = "SELECT * FROM RANDOM;";
    int rowCount;

    @PostConstruct
    public void initialize() {
        // Load the Informix Driver
        try {
            Class.forName("com.informix.jdbc.IfxDriver");
            System.out.println("SUCCESS: loaded Informix Driver.");
        } catch (Exception e) {
            System.out.println("ERROR: failed to load Informix JDBC driver.");
            e.printStackTrace();
        }

        // Connect to Informix DB
        try {
            String url = "jdbc:informix-sqli://127.0.0.1:60001/demo:INFORMIXSERVER=inst_1;user=root;password=infm$$fis";
            conn = DriverManager.getConnection(url);
            System.out.println("SUCCESS: established Informix connection.");
            return;
        } catch (Exception e) {
            System.out.println("ERROR: failed to connect!");
            System.out.println("ERROR: " + e.getMessage());
            e.printStackTrace();
            return;
        }
    }

    @Schedule(second = "*/20", minute = "*", hour = "*", persistent = false)
    public void doWork() {
        Date currentTime = new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy.MM.dd G 'at' HH:mm:ss z");
        String randomId = RandomStringUtils.randomAlphabetic(10);
        String writeQuery = "INSERT INTO RANDOM values (\"" + randomId + "\");";

        // Run a SQL INSERT query
        try {
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(writeQuery);
            stmt.close();
            System.out.println("SUCCESS: insert query completed - " + randomId + " at " +  simpleDateFormat.format(currentTime));
        } catch (SQLException s) {
            System.out.println("ERROR: insert query failed at " + simpleDateFormat.format(currentTime));
            System.out.println("ERROR: " + s.getMessage());
            return;
        }

        // Run a SQL SELECT query
        try {
            PreparedStatement pstmt = conn.prepareStatement(selectQuery);
            ResultSet r = pstmt.executeQuery();
            while(r.next()) {
                String id = r.getString(1);
                System.out.println("Select: column a = " + id);
            }
            r.close();
            pstmt.close();
            System.out.println("SUCCESS: select query completed at " + simpleDateFormat.format(currentTime));
        } catch (SQLException ss) {
            System.out.println("ERROR: select query failed at " + simpleDateFormat.format(currentTime));
            System.out.println("ERROR: " + ss.getMessage());
            return;
        }
    }
}