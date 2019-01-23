/*
 * Copyright 2016 Joao Vicente
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apm4all.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.*;

public class UrlReader {
    private final String urlString;

    public UrlReader(String url)    {
        this.urlString = url;
    }

    public String read() {
        StringBuilder returnedJSON = new StringBuilder();
        String outputLine;
        BufferedReader bufferedReader = null;
        int httpResponseCode;
        String errorMessage = "";

        try {
            URL url = new URL(urlString);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setDoInput(true);
            connection.setDoOutput(true);
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Accept", "application/json");
            connection.setRequestProperty("Content-Type", "application/json; charset=UTF-8");

            connection.connect();
            httpResponseCode = connection.getResponseCode();

            if (httpResponseCode == HttpURLConnection.HTTP_OK) {

                bufferedReader = new BufferedReader(new InputStreamReader(connection.getInputStream()));

            } else if (httpResponseCode == HttpURLConnection.HTTP_GATEWAY_TIMEOUT
                    | httpResponseCode == HttpURLConnection.HTTP_CLIENT_TIMEOUT) {
                errorMessage = "Hadoop job History server gateway timeout";
                bufferedReader = new BufferedReader(new InputStreamReader(connection.getErrorStream()));

            } else if (httpResponseCode == HttpURLConnection.HTTP_BAD_REQUEST) {
                errorMessage = "Hadoop job History bad request";
                bufferedReader = new BufferedReader(new InputStreamReader(connection.getErrorStream()));

            } else if (httpResponseCode == HttpURLConnection.HTTP_NOT_FOUND) {
                errorMessage = "Hadoop job History URL NOT FOUND";
                bufferedReader = new BufferedReader(new InputStreamReader(connection.getErrorStream()));
            }
            else if (httpResponseCode == HttpURLConnection.HTTP_INTERNAL_ERROR) {
                errorMessage = "Internal Server Error";
                bufferedReader = new BufferedReader(new InputStreamReader(connection.getErrorStream()));
            }
            else {
                errorMessage = "HTTP response code: " + httpResponseCode;
                bufferedReader = new BufferedReader(new InputStreamReader(connection.getErrorStream()));
            }
            
            while ((outputLine = bufferedReader.readLine()) != null) {
                returnedJSON.append(outputLine);

            }

            if (bufferedReader != null && connection != null) {
                bufferedReader.close();
                connection.disconnect();
            }

        } catch (ConnectException ce) {
            errorMessage = "Hadoop job History internal server error or unavailable";
        } catch (ProtocolException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            errorMessage = "Malformed URL";
            e.printStackTrace();
        } catch (IOException e) {
            errorMessage = "Malformed URL";
            e.printStackTrace();
        }
        return returnedJSON.toString();
    }
}
