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
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.VanillaChronicle;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.apache.synapse.message.MessageConsumer;

public class ChronicleConsumer implements MessageConsumer {

	private static final Log logger = LogFactory
			.getLog(ChronicleConsumer.class);

	private final ChronicleStore store;

	/** ID of this message consumer instance */
	private String idString;

	private MessageContext lastMessage;

	private final Object chronicleLock;

	private static ExcerptTailer dataTailer;

	private static ExcerptTailer indexTailer;

	private static long dataTailerLastIndex;

	private VanillaChronicle chronicleIndex;

	public ChronicleConsumer(ChronicleStore store,
			VanillaChronicle chronicleData, VanillaChronicle chronicleIndex)
			throws IOException {
		this.store = store;
		this.chronicleLock = store.getChronicleLock();
		setDataTailer(chronicleData.createTailer());
		setIndexTailer(chronicleIndex.createTailer());
		this.chronicleIndex = chronicleIndex;
	}

	public MessageContext receive() {
		MessageContext message = null;
	//	synchronized (chronicleLock) {
			long lastReadIndexData = -1L;
			try {
				lastReadIndexData = lastReadIndex();
			} catch (IOException e) {
				logger.error("Error while reading lastReadIndex "
						+ e.getLocalizedMessage());
			}
			while (getDataTailer().nextIndex()) {
				if (getDataTailer().index() > lastReadIndexData) {
					dataTailerLastIndex = getDataTailer().index();
					Long dateWrite = getDataTailer().readLong();
					StorableMessage storableMessage = (StorableMessage) getDataTailer()
							.readObject();
					org.apache.axis2.context.MessageContext axis2Mc = store
							.newAxis2Mc();
					message = store.newSynapseMc(axis2Mc);
					message = MessageConverter.toMessageContext(
							storableMessage, axis2Mc, message);
					getDataTailer().finish();
					break;
				}
			}

			if (logger.isDebugEnabled()) {
				if (message != null) {
					if (logger.isDebugEnabled()) {
						logger.debug(getId() + " received MessageID : "
								+ message.getMessageID());
					}
				}
			}
			lastMessage = message;
	//	}
		return message;
	}

	/**
	 * @return index of last consumed message
	 * @throws IOException
	 */
	public long lastReadIndex() throws IOException {
		while (getIndexTailer().index(chronicleIndex.lastIndex())) {
			long l = getIndexTailer().readLong();
			getIndexTailer().finish();
			return l;
		}
		return -1L;
	}

	public boolean ack() {
		if (logger.isDebugEnabled() && lastMessage != null) {
			logger.debug(getId() + " ack");
		}
	//	synchronized (chronicleLock) {
			try {
				store.setindex(dataTailerLastIndex);
				store.dequeued();
				store.clearDirectories();
			} catch (Exception e) {
				e.printStackTrace();
			}
	//	}
		return true;
	}

	public boolean cleanup() {
		 if (logger.isDebugEnabled()) {
	            logger.debug(getId() + " cleaning up...");
	        }

	        return false;
	}

	public void setId(int i) {
		idString = "[" + store.getName() + "-C-" + i + "]";

	}

	public String getId() {
		return idString;
	}

	public static ExcerptTailer getDataTailer() {
		return dataTailer;
	}

	public static void setDataTailer(ExcerptTailer dataTailer) {
		ChronicleConsumer.dataTailer = dataTailer;
	}

	public static ExcerptTailer getIndexTailer() {
		return indexTailer;
	}

	public static void setIndexTailer(ExcerptTailer indexTailer) {
		ChronicleConsumer.indexTailer = indexTailer;
	}

}
