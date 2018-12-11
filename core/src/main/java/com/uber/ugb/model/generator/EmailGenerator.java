/*
 *
 *  * Copyright 2018 Uber Technologies Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.uber.ugb.model.generator;

public class EmailGenerator extends Generator<String> {

    private static String randomText = "abcdefghijklmnopqrstuvwxyz";
    private static String[] domains = {"gmail.com", "yahoo.com", "outlook.com", "inbox.com", "icloud.com", "mail.com"};

    @Override
    protected String genValue() {
        int lengh = random.nextInt(8) + 3;

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < lengh; i++) {
            int x = random.nextInt(randomText.length());
            sb.append(randomText.charAt(x));
        }
        sb.append("@");
        sb.append(domains[random.nextInt(domains.length)]);

        return sb.toString();
    }
}
