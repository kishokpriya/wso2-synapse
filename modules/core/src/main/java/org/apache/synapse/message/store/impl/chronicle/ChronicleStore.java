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

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.VanillaChronicle;
import net.openhft.chronicle.VanillaChronicleConfig;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.apache.synapse.config.SynapseConfiguration;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.core.axis2.Axis2SynapseEnvironment;
import org.apache.synapse.message.MessageConsumer;
import org.apache.synapse.message.MessageProducer;
import org.apache.synapse.message.store.AbstractMessageStore;
import org.apache.commons.io.FileUtils;

public class ChronicleStore extends AbstractMessageStore {

	private static final Log logger = LogFactory.getLog(ChronicleStore.class);

	/**
	 * Chronicle to hold the message
	 */
	private static VanillaChronicle chronicalData;
	/**
	 * Chronicle to hold details of last read message
	 */
	private static VanillaChronicle chronicalIndex;

	private ChronicleProducer chronicalProducer;

	private ChronicleConsumer chronicalConsumer;

	private VanillaChronicleConfig config;

	private final Object chronicleLock = new Object();
	/**
	 * File containing the message
	 */
	private File dataFile;
	/**
	 * File containing the last read message index
	 */
	private File indexFile;
	/**
	 * To check whether all the messages are consumed before delete the file
	 */
	private static boolean isAllConsumed = false;

	public MessageProducer getProducer() {
		try {
			ChronicleProducer producer = new ChronicleProducer(this,
					chronicalData, chronicalIndex);
			producer.setId(nextProducerId());
			logger.info(nameString()
					+ " created a new VanillaChorical Message Producer.");
			this.chronicalProducer = producer;
			return producer;
		} catch (IOException e) {
			logger.error("Could not create a Message Producer for "
					+ nameString() + ". Error:" + e.getLocalizedMessage());
			return null;
		}
	}

	public MessageConsumer getConsumer() {
		try {
			ChronicleConsumer consumer = new ChronicleConsumer(this,
					chronicalData, chronicalIndex);
			consumer.setId(nextConsumerId());
			logger.info(nameString()
					+ " created a new VanillaChorical Message Consumer.");
			this.chronicalConsumer = consumer;
			return consumer;
		} catch (IOException e) {
			logger.error("Could not create a Message Consumer for "
					+ nameString() + ". Error:" + e.getLocalizedMessage());
			return null;
		}
	}

	/**
	 * @param differenceOfPreviousDay
	 *            - Used to identify the directory to check the messages whether
	 *            all messages are consumed
	 * @param dateRead
	 *            - currentTimeMillis
	 * @return
	 * @throws IOException
	 * @throws ParseException
	 */
	public boolean isAllMessagedConsumed(int differenceOfPreviousDay,
			String dateRead) throws IOException, ParseException {
		//synchronized (chronicleLock) {
			long lastReadIndex = chronicalConsumer.lastReadIndex();
			ExcerptTailer tailer = chronicalData.createTailer();
			while (tailer.nextIndex()) {
				Long writeDate = tailer.readLong();
				Date messageWriteDate = new Date(writeDate);
				String dateWrite = new SimpleDateFormat("yyyyMMdd")
						.format(messageWriteDate);

				SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");

				Date write, read = null;
				write = format.parse(dateWrite);
				read = format.parse(dateRead);

				// in milliseconds
				long consumedDiffrence = read.getTime() - write.getTime();
				long diffDays = consumedDiffrence / (24 * 60 * 60 * 1000);

				// check whether all messages consumed in a particular day
				if (diffDays != differenceOfPreviousDay) {
					isAllConsumed = true;
					break;
				} else {
					if (diffDays > 0) {
						if (tailer.index() < lastReadIndex) {
							isAllConsumed = true;
						} else {
							isAllConsumed = false;
							break;
						}
					}
				}

				tailer.finish();

			}
		//}
		return isAllConsumed;
	}

	public void setindex(long index) throws IOException {
		chronicalProducer.saveIndex(index);
	}

	/**
	 * Delete the directories of past days, if all the messages are consumed.
	 */
	public void clearDirectories() {
		String[] directories = getDataFile().list();
		Arrays.sort(directories);
		if (directories.length > 1) {
			try {
				String todayDirectory = new SimpleDateFormat("yyyyMMdd")
						.format(System.currentTimeMillis());
				for (String directory : directories) {
					if (!directory.equalsIgnoreCase(todayDirectory)) {
						int differenceOfPreviousDay = Integer
								.parseInt(todayDirectory)
								- Integer.parseInt(directory.toString());
						File dataSubFile = new File(getDataFile()
								+ File.separator + directory.toString());
						File indexSubFile = new File(getIndexFile()
								+ File.separator + directory.toString());

						boolean isAllMessagesConsumed = isAllMessagedConsumed(
								differenceOfPreviousDay, todayDirectory);

						if (isAllMessagesConsumed) {
							FileUtils.deleteDirectory(dataSubFile);
							FileUtils.deleteDirectory(indexSubFile);

						} else
							break;

					}
				}
			} catch (IOException e) {
				logger.error(nameString() + "Error while delteting old files "
						+ e.getLocalizedMessage());
			} catch (ParseException e) {
				logger.error(nameString()
						+ " Error while converting String to int "
						+ e.getLocalizedMessage());
			}
		}
	}

	public void clear() {

	}

	public MessageContext remove() throws NoSuchElementException {
		// TODO Auto-generated method stub
		return null;
	}

	public MessageContext remove(String messageID) {
		// TODO Auto-generated method stub
		return null;
	}

	public MessageContext get(int index) {
		// TODO Auto-generated method stub
		return null;
	}

	public List<MessageContext> getAll() {
		// TODO Auto-generated method stub
		return null;
	}

	public MessageContext get(String messageId) {
		// TODO Auto-generated method stub
		return null;
	}

	public void init(SynapseEnvironment se) {
		if (se == null) {
			logger.error("Cannot initialize store.");
			return;
		}
		super.init(se);
		if (createChronicle(getName())) {
			logger.info(nameString() + ". Initialized ...");
		} else {
			logger.info(nameString() + ". Initialization failed...");
		}
	}

	@Override
	public void setParameters(Map<String, Object> parameters) {
		super.setParameters(parameters);
	}

	/**
	 * @param storeName
	 *            - The Chronicle store name
	 * @return true if the Data & Index chronicles are created
	 */
	private boolean createChronicle(String storeName) {
		String basePrefixData, basePrefixIndex;
		if (this.getParameters().get(ChronicleConstants.EXTERNAL_FILE_LOCATION) != null) {
			String location = this.getParameters()
					.get(ChronicleConstants.EXTERNAL_FILE_LOCATION).toString();
			if ((location.substring(location.length() - 1)).equals("/")) {
				basePrefixData = location + ChronicleConstants.DATA_DIRECTORY
						+ File.separator + storeName;
				basePrefixIndex = location + ChronicleConstants.INDEX_DIRECTORY
						+ File.separator + storeName;
			} else {
				basePrefixData = location + File.separator
						+ ChronicleConstants.DATA_DIRECTORY + File.separator
						+ storeName;
				basePrefixIndex = location + File.separator
						+ ChronicleConstants.INDEX_DIRECTORY + File.separator
						+ storeName;
			}

		} else {
			basePrefixData = System.getProperty("user.dir") + File.separator
					+ ChronicleConstants.DEFAULT_DIRECTORY + File.separator
					+ ChronicleConstants.DATA_DIRECTORY + File.separator
					+ storeName;
			basePrefixIndex = System.getProperty("user.dir") + File.separator
					+ ChronicleConstants.DEFAULT_DIRECTORY + File.separator
					+ ChronicleConstants.INDEX_DIRECTORY + File.separator
					+ storeName;
		}

		try {
			config = new VanillaChronicleConfig();
			config.defaultMessageSize(1024 << 10);
			chronicalData = new VanillaChronicle(basePrefixData, config);
			chronicalIndex = new VanillaChronicle(basePrefixIndex);
			dataFile = new File(basePrefixData);
			indexFile = new File(basePrefixIndex);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	private VanillaChronicle getDataChronicle() {
		return chronicalData;
	}

	private String nameString() {
		return "Store [" + getName() + "]";
	}

	public Object getChronicleLock() {
		return chronicleLock;
	}

	public org.apache.axis2.context.MessageContext newAxis2Mc() {
		return ((Axis2SynapseEnvironment) synapseEnvironment)
				.getAxis2ConfigurationContext().createMessageContext();
	}

	public org.apache.synapse.MessageContext newSynapseMc(
			org.apache.axis2.context.MessageContext msgCtx) {
		SynapseConfiguration configuration = synapseEnvironment
				.getSynapseConfiguration();
		return new Axis2MessageContext(msgCtx, configuration,
				synapseEnvironment);
	}

	
	 public void destroy() {
	        if (logger.isDebugEnabled()) {
	            logger.debug("Destroying " + nameString() + "...");
	        }
	        chronicalData.clear();
	        chronicalData.close();
	        chronicalIndex.clear();
	        chronicalIndex.close();
	        super.destroy();
	    }
	 
	 public boolean cleanup()
	 {
		 chronicalData.clear();
		 chronicalData.close();
		 chronicalIndex.clear();
	     chronicalIndex.close();
		return true;
		 
	 }

	public File getDataFile() {
		return dataFile;
	}

	public File getIndexFile() {
		return indexFile;
	}

	public void setDataFile(File dataFile) {
		this.dataFile = dataFile;
	}

	public void setIndexFile(File indexFile) {
		this.indexFile = indexFile;
	}

	public boolean deleteDirectory(File path) {
		if (path.exists()) {
			File[] files = path.listFiles();
			for (int i = 0; i < files.length; i++) {
				if (files[i].isDirectory()) {
					deleteDirectory(files[i]);
				} else {
					files[i].delete();
				}
			}
		}
		return (path.delete());
	}
}
