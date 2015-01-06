/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *   * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.synapse.message.store.impl.chronicle;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.VanillaChronicle;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.apache.synapse.message.MessageProducer;

public class ChronicleProducer implements MessageProducer {

	private static final Log logger = LogFactory
			.getLog(ChronicleProducer.class);

	private final ChronicleStore store;

	private String idString;

	private final Object chronicleLock;

	private ExcerptAppender dataAppender;

	private ExcerptAppender indexAppender;

	/**
	 * @param store
	 *            - Chronicle store
	 * @param chronicleData
	 *            - The chronicle to save messages
	 * @param chronicalIndex
	 *            - The chronicle to save consumed message index
	 * @throws IOException
	 */
	public ChronicleProducer(ChronicleStore store,
			VanillaChronicle chronicleData, VanillaChronicle chronicalIndex)
			throws IOException {
		this.store = store;
		this.chronicleLock = store.getChronicleLock();
		this.dataAppender = chronicleData.createAppender();
		this.indexAppender = chronicalIndex.createAppender();
	}

	public boolean storeMessage(MessageContext synCtx) {
		if (synCtx == null) {
			return false;
		}
		boolean result = false;
		StorableMessage message = MessageConverter.toStorableMessage(synCtx);
		if (message != null) {
		//	synchronized (chronicleLock) {
				dataAppender.startExcerpt();
				dataAppender.writeLong(System.currentTimeMillis());
				dataAppender.writeObject(message);
				dataAppender.finish();
				result = true;
		//	}
			if (!result) {
				logger.warn(getId() + " ignored MessageID : "
						+ synCtx.getMessageID());
			}
		}

		if (logger.isDebugEnabled()) {
			logger.debug(getId() + " stored MessageID: "
					+ synCtx.getMessageID());
		}
		if (result) {
			store.enqueued();
		}
		return result;
	}

	/**
	 * @param index
	 *            - last message index consumed from the data file
	 * @throws IOException
	 */
	public void saveIndex(long index) throws IOException {
		indexAppender.startExcerpt();
		indexAppender.writeLong(index);
		indexAppender.finish();
	}

	public boolean cleanup() {
		if (logger.isDebugEnabled()) {
            logger.debug(getId() + " cleanup");
        }
        return true;
	}

	public void setId(int id) {
		idString = "[" + store.getName() + "-P-" + id + "]";

	}

	public String getId() {
		return idString;
	}
}
