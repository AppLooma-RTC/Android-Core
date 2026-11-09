/*
 * Copyright 2023-2025 Transport, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.applooma.rtc.transport.room

import android.os.SystemClock
import androidx.annotation.VisibleForTesting
import com.google.protobuf.ByteString
import com.vdurmont.semver4j.Semver
import com.applooma.rtc.transport.ConnectOptions
import com.applooma.rtc.transport.RoomOptions
import com.applooma.rtc.transport.dagger.InjectionNames
import com.applooma.rtc.transport.events.DisconnectReason
import com.applooma.rtc.transport.events.convert
import com.applooma.rtc.transport.room.participant.ParticipantTrackPermission
import com.applooma.rtc.transport.room.track.TrackException
import com.applooma.rtc.transport.room.util.MediaConstraintKeys
import com.applooma.rtc.transport.room.util.createAnswer
import com.applooma.rtc.transport.room.util.setLocalDescription
import com.applooma.rtc.transport.room.util.waitUntilConnected
import com.applooma.rtc.transport.util.CloseableCoroutineScope
import com.applooma.rtc.transport.util.Either
import com.applooma.rtc.transport.util.FlowObservable
import com.applooma.rtc.transport.util.LKLog
import com.applooma.rtc.transport.util.flowDelegate
import com.applooma.rtc.transport.util.nullSafe
import com.applooma.rtc.transport.util.withCheckLock
import com.applooma.rtc.transport.webrtc.RTCStatsGetter
import com.applooma.rtc.transport.webrtc.copy
import com.applooma.rtc.transport.webrtc.isConnected
import com.applooma.rtc.transport.webrtc.isDisconnected
import com.applooma.rtc.transport.webrtc.peerconnection.executeBlockingOnRTCThread
import com.applooma.rtc.transport.webrtc.peerconnection.launchBlockingOnRTCThread
import com.applooma.rtc.transport.webrtc.toProtoSessionDescription
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.coroutines.yield
import com.applooma.rtc.transport.proto.Models
import com.applooma.rtc.transport.proto.Models.AudioTrackFeature
import com.applooma.rtc.transport.proto.Rtc
import com.applooma.rtc.transport.proto.Rtc.JoinResponse
import com.applooma.rtc.transport.proto.Rtc.LeaveRequest
import com.applooma.rtc.transport.proto.Rtc.ReconnectResponse
import org.webrtc.DataChannel
import org.webrtc.IceCandidate
import org.webrtc.MediaConstraints
import org.webrtc.MediaStream
import org.webrtc.MediaStreamTrack
import org.webrtc.PeerConnection
import org.webrtc.PeerConnection.PeerConnectionState
import org.webrtc.PeerConnection.RTCConfiguration
import org.webrtc.RTCStatsCollectorCallback
import org.webrtc.RTCStatsReport
import org.webrtc.RtpReceiver
import org.webrtc.RtpSender
import org.webrtc.RtpTransceiver
import org.webrtc.RtpTransceiver.RtpTransceiverInit
import org.webrtc.SessionDescription
import java.net.ConnectException
import java.nio.ByteBuffer
import javax.inject.Inject
import javax.inject.Named
import javax.inject.Singleton
import kotlin.coroutines.Continuation
import kotlin.coroutines.resume
import kotlin.coroutines.suspendCoroutine

/**
 * @suppress
 */
@Singleton
class RTCEngine
@Inject
internal constructor(
    val client: SignalClient,
    private val pctFactory: PeerConnectionTransport.Factory,
    @Named(InjectionNames.DISPATCHER_IO)
    private val ioDispatcher: CoroutineDispatcher,
) : SignalClient.Listener {
    internal var listener: Listener? = null

    /**
     * Reflects the combined connection state of SignalClient and primary PeerConnection.
     */
    @FlowObservable
    @VisibleForTesting(otherwise = VisibleForTesting.PACKAGE_PRIVATE)
    var connectionState: ConnectionState by flowDelegate(ConnectionState.DISCONNECTED) { newVal, oldVal ->
        if (newVal == oldVal) {
            return@flowDelegate
        }
        when (newVal) {
            ConnectionState.CONNECTED -> {
                if (oldVal == ConnectionState.DISCONNECTED) {
                    LKLog.d { "primary ICE connected" }
                    listener?.onEngineConnected()
                } else if (oldVal == ConnectionState.RECONNECTING) {
                    LKLog.d { "primary ICE reconnected" }
                    listener?.onEngineReconnected()
                } else if (oldVal == ConnectionState.RESUMING) {
                    listener?.onEngineResumed()
                }
            }

            ConnectionState.DISCONNECTED -> {
                LKLog.d { "primary ICE disconnected" }
                if (oldVal == ConnectionState.CONNECTED) {
                    reconnect()
                }
            }

            else -> {
            }
        }
    }
    internal var reconnectType: ReconnectType = ReconnectType.DEFAULT
    private var reconnectingJob: Job? = null
    private var fullReconnectOnNext = false

    private val pendingTrackResolvers: MutableMap<String, Continuation<Models.TrackInfo>> =
        mutableMapOf()

    internal var regionUrlProvider: RegionUrlProvider? = null
    private var sessionUrl: String? = null
    private var sessionToken: String? = null
    private var connectOptions: ConnectOptions? = null
    private var lastRoomOptions: RoomOptions? = null
    private var participantSid: String? = null

    internal val serverVersion: Semver?
        get() = client.serverVersion

    private val publisherObserver = PublisherTransportObserver(this, client)
    private val subscriberObserver = SubscriberTransportObserver(this, client)

    private var publisher: PeerConnectionTransport? = null
    private var subscriber: PeerConnectionTransport? = null

    private var reliableDataChannel: DataChannel? = null
    private var reliableDataChannelSub: DataChannel? = null
    private var lossyDataChannel: DataChannel? = null
    private var lossyDataChannelSub: DataChannel? = null

    private var isSubscriberPrimary = false
    private var isClosed = true

    private var hasPublished = false

    private var coroutineScope = CloseableCoroutineScope(SupervisorJob() + ioDispatcher)

    /**
     * Note: If this lock is ever used in conjunction with the RTC thread,
     * this must be grabbed on the RTC thread to prevent deadlocks.
     */
    private var configurationLock = Mutex()

    init {
        client.listener = this
    }

    suspend fun join(
        url: String,
        token: String,
        options: ConnectOptions,
        roomOptions: RoomOptions,
    ): JoinResponse {
        coroutineScope.close()
        coroutineScope = CloseableCoroutineScope(SupervisorJob() + ioDispatcher)
        sessionUrl = url
        sessionToken = token
        connectOptions = options
        lastRoomOptions = roomOptions
        return joinImpl(url, token, options, roomOptions)
    }

    suspend fun joinImpl(
        url: String,
        token: String,
        options: ConnectOptions,
        roomOptions: RoomOptions,
    ): JoinResponse = coroutineScope {
        val joinResponse = client.join(url, token, options, roomOptions)
        ensureActive()

        listener?.onJoinResponse(joinResponse)
        isClosed = false
        listener?.onSignalConnected(false)

        isSubscriberPrimary = joinResponse.subscriberPrimary

        configure(joinResponse, options)

        // create offer
        if (!isSubscriberPrimary) {
            negotiatePublisher()
        }
        client.onReadyForResponses()

        return@coroutineScope joinResponse
    }

    private suspend fun configure(joinResponse: JoinResponse, connectOptions: ConnectOptions) {
        launchBlockingOnRTCThread {
            configurationLock.withCheckLock(
                {
                    ensureActive()
                    if (publisher != null && subscriber != null) {
                        // already configured
                        return@launchBlockingOnRTCThread
                    }
                },
            ) {
                participantSid = if (joinResponse.hasParticipant()) {
                    joinResponse.participant.sid
                } else {
                    null
                }

                // Setup peer connections
                val rtcConfig = makeRTCConfig(Either.Left(joinResponse), connectOptions)

                publisher?.close()
                publisher = pctFactory.create(
                    rtcConfig,
                    publisherObserver,
                    publisherObserver,
                )
                subscriber?.close()
                subscriber = pctFactory.create(
                    rtcConfig,
                    subscriberObserver,
                    null,
                )

                val connectionStateListener: PeerConnectionStateListener = { newState ->
                    LKLog.v { "onIceConnection new state: $newState" }
                    if (newState.isConnected()) {
                        connectionState = ConnectionState.CONNECTED
                    } else if (newState.isDisconnected()) {
                        connectionState = ConnectionState.DISCONNECTED
                    }
                }

                if (joinResponse.subscriberPrimary) {
                    // in subscriber primary mode, server side opens sub data channels.
                    subscriberObserver.dataChannelListener = onDataChannel@{ dataChannel: DataChannel ->
                        when (dataChannel.label()) {
                            RELIABLE_DATA_CHANNEL_LABEL -> reliableDataChannelSub = dataChannel
                            LOSSY_DATA_CHANNEL_LABEL -> lossyDataChannelSub = dataChannel
                            else -> return@onDataChannel
                        }
                        dataChannel.registerObserver(DataChannelObserver(dataChannel))
                    }

                    subscriberObserver.connectionChangeListener = connectionStateListener
                    // Also reconnect on publisher disconnect
                    publisherObserver.connectionChangeListener = { newState ->
                        if (newState.isDisconnected()) {
                            reconnect()
                        }
                    }
                } else {
                    publisherObserver.connectionChangeListener = connectionStateListener
                }

                ensureActive()
                // data channels
                val reliableInit = DataChannel.Init()
                reliableInit.ordered = true
                reliableDataChannel = publisher?.withPeerConnection {
                    createDataChannel(
                        RELIABLE_DATA_CHANNEL_LABEL,
                        reliableInit,
                    ).also { dataChannel ->
                        dataChannel.registerObserver(DataChannelObserver(dataChannel))
                    }
                }

                ensureActive()
                val lossyInit = DataChannel.Init()
                lossyInit.ordered = true
                lossyInit.maxRetransmits = 0
                lossyDataChannel = publisher?.withPeerConnection {
                    createDataChannel(
                        LOSSY_DATA_CHANNEL_LABEL,
                        lossyInit,
                    ).also { dataChannel ->
                        dataChannel.registerObserver(DataChannelObserver(dataChannel))
                    }
                }
            }
        }
    }

    /**
     * @param builder an optional builder to include other parameters related to the track
     */
    suspend fun addTrack(
        cid: String,
        name: String,
        kind: Models.TrackType,
        stream: String?,
        builder: Rtc.AddTrackRequest.Builder = Rtc.AddTrackRequest.newBuilder(),
    ): Models.TrackInfo {
        if (pendingTrackResolvers[cid] != null) {
            throw TrackException.DuplicateTrackException("Track with same ID $cid has already been published!")
        }
        // Suspend until signal client receives message confirming track publication.
        return suspendCoroutine { cont ->
            pendingTrackResolvers[cid] = cont
            client.sendAddTrack(
                cid = cid,
                name = name,
                type = kind,
                stream = stream,
                builder = builder,
            )
        }
    }

    internal suspend fun createSenderTransceiver(
        rtcTrack: MediaStreamTrack,
        transInit: RtpTransceiverInit,
    ): RtpTransceiver? {
        return publisher?.withPeerConnection {
            addTransceiver(rtcTrack, transInit)
        }
    }

    fun updateSubscriptionPermissions(
        allParticipants: Boolean,
        participantTrackPermissions: List<ParticipantTrackPermission>,
    ) {
        client.sendUpdateSubscriptionPermissions(allParticipants, participantTrackPermissions)
    }

    fun updateMuteStatus(sid: String, muted: Boolean) {
        client.sendMuteTrack(sid, muted)
    }

    fun updateLocalAudioTrack(sid: String, features: Collection<AudioTrackFeature>) {
        client.sendUpdateLocalAudioTrack(sid, features)
    }

    fun close(reason: String = "Normal Closure") {
        if (isClosed) {
            return
        }
        LKLog.v { "Close - $reason" }
        isClosed = true
        reconnectingJob?.cancel()
        reconnectingJob = null
        coroutineScope.close()
        hasPublished = false
        sessionUrl = null
        sessionToken = null
        connectOptions = null
        lastRoomOptions = null
        participantSid = null
        regionUrlProvider = null
        closeResources(reason)
        connectionState = ConnectionState.DISCONNECTED
    }

    private fun closeResources(reason: String) {
        executeBlockingOnRTCThread {
            runBlocking {
                configurationLock.withLock {
                    publisherObserver.connectionChangeListener = null
                    subscriberObserver.connectionChangeListener = null
                    publisher?.closeBlocking()
                    publisher = null
                    subscriber?.closeBlocking()
                    subscriber = null

                    fun DataChannel?.completeDispose() {
                        this?.unregisterObserver()
                        this?.close()
                        this?.dispose()
                    }

                    reliableDataChannel?.completeDispose()
                    reliableDataChannel = null
                    reliableDataChannelSub?.completeDispose()
                    reliableDataChannelSub = null
                    lossyDataChannel?.completeDispose()
                    lossyDataChannel = null
                    lossyDataChannelSub?.completeDispose()
                    lossyDataChannelSub = null
                    isSubscriberPrimary = false
                }
            }
        }
        client.close(reason = reason)
    }

    /**
     * reconnect Signal and PeerConnections
     */
    @Synchronized
    @VisibleForTesting
    fun reconnect() {
        if (reconnectingJob?.isActive == true) {
            LKLog.d { "Reconnection is already in progress" }
            return
        }
        if (this.isClosed) {
            LKLog.d { "Skip reconnection - engine is closed" }
            return
        }
        var url = sessionUrl
        val token = sessionToken
        if (url == null || token == null) {
            LKLog.w { "couldn't reconnect, no url or no token" }
            return
        }
        val forceFullReconnect = fullReconnectOnNext
        fullReconnectOnNext = false
        val job = coroutineScope.launch {
            var hasResumedOnce = false
            var hasReconnectedOnce = false

            val reconnectStartTime = SystemClock.elapsedRealtime()
            for (retries in 0 until MAX_RECONNECT_RETRIES) {
                // First try use previously valid url.
                if (retries != 0) {
                    try {
                        url = regionUrlProvider?.getNextBestRegionUrl() ?: url
                    } catch (e: Exception) {
                        LKLog.d(e) { "Exception while getting next best region url while reconnecting." }
                    }
                }

                ensureActive()
                if (retries != 0) {
                    yield()
                }

                if (isClosed) {
                    LKLog.v { "RTCEngine closed, aborting reconnection" }
                    break
                }

                var startDelay = 100 + retries.toLong() * retries * 500
                if (startDelay > 5000) {
                    startDelay = 5000
                }

                LKLog.i { "Reconnecting to signal, attempt ${retries + 1}" }
                delay(startDelay)

                val isFullReconnect = when (reconnectType) {
                    // full reconnect after first try.
                    ReconnectType.DEFAULT -> retries != 0 || forceFullReconnect
                    ReconnectType.FORCE_SOFT_RECONNECT -> false
                    ReconnectType.FORCE_FULL_RECONNECT -> true
                }

                val connectOptions = connectOptions ?: ConnectOptions()
                if (isFullReconnect) {
                    LKLog.v { "Attempting full reconnect." }

                    if (!hasReconnectedOnce) {
                        hasReconnectedOnce = true
                        listener?.onEngineReconnecting()
                    }
                    connectionState = ConnectionState.RECONNECTING
                    try {
                        closeResources("Full Reconnecting")
                        listener?.onFullReconnecting()
                        joinImpl(url!!, token, connectOptions, lastRoomOptions ?: RoomOptions())
                    } catch (e: Exception) {
                        LKLog.w(e) { "Error during reconnection." }
                        // reconnect failed, retry.
                        continue
                    }
                } else {
                    if (!hasResumedOnce) {
                        hasResumedOnce = true
                        listener?.onEngineResuming()
                    }
                    connectionState = ConnectionState.RESUMING
                    LKLog.v { "Attempting soft reconnect." }
                    subscriber?.prepareForIceRestart()
                    try {
                        val response = client.reconnect(url!!, token, participantSid)
                        if (response is Either.Left) {
                            val reconnectResponse = response.value
                            val rtcConfig = makeRTCConfig(Either.Right(reconnectResponse), connectOptions)
                            subscriber?.updateRTCConfig(rtcConfig)
                            publisher?.updateRTCConfig(rtcConfig)
                        }
                        client.onReadyForResponses()
                    } catch (e: Exception) {
                        LKLog.w(e) { "Error during reconnection." }
                        // ws reconnect failed, retry.
                        continue
                    }

                    LKLog.v { "ws reconnected, restarting ICE" }
                    listener?.onSignalConnected(!isFullReconnect)

                    // trigger publisher reconnect
                    // only restart publisher if it's needed
                    if (hasPublished) {
                        negotiatePublisher()
                    }
                }

                ensureActive()
                if (isClosed) {
                    LKLog.v { "RTCEngine closed, aborting reconnection" }
                    break
                }

                // wait until publisher ICE connected
                var publisherWaitJob: Job? = null
                if (hasPublished) {
                    publisherWaitJob = launch {
                        publisherObserver.waitUntilConnected()
                    }
                }

                // wait until subscriber ICE connected
                val subscriberWaitJob = launch {
                    subscriberObserver.waitUntilConnected()
                }

                withTimeoutOrNull(MAX_ICE_CONNECT_TIMEOUT_MS.toLong()) {
                    listOfNotNull(publisherWaitJob, subscriberWaitJob)
                        .joinAll()
                }

                ensureActive()
                if (isClosed) {
                    LKLog.v { "RTCEngine closed, aborting reconnection" }
                    break
                }

                if (connectionState == ConnectionState.CONNECTED &&
                    (!hasPublished || publisher?.isConnected() == true)
                ) {
                    // Is connected, notify and return.
                    regionUrlProvider?.clearAttemptedRegions()
                    client.onPCConnected()
                    listener?.onPostReconnect(isFullReconnect)
                    return@launch
                }

                // Didn't manage to reconnect, check if should continue to next attempt.
                val curReconnectTime = SystemClock.elapsedRealtime() - reconnectStartTime
                if (curReconnectTime > MAX_RECONNECT_TIMEOUT) {
                    break
                }
            }

            close("Failed reconnecting")
            listener?.onEngineDisconnected(DisconnectReason.UNKNOWN_REASON)
        }

        reconnectingJob = job
        job.invokeOnCompletion {
            if (reconnectingJob == job) {
                reconnectingJob = null
            }
        }
    }

    internal fun negotiatePublisher() {
        if (!client.isConnected) {
            return
        }

        hasPublished = true

        coroutineScope.launch {
            publisher?.negotiate?.invoke(getPublisherOfferConstraints())
        }
    }

    internal suspend fun sendData(dataPacket: Models.DataPacket) {
        ensurePublisherConnected(dataPacket.kind)

        val buf = DataChannel.Buffer(
            ByteBuffer.wrap(dataPacket.toByteArray()),
            true,
        )

        val channel = dataChannelForKind(dataPacket.kind)
            ?: throw TrackException.PublishException("channel not established for ${dataPacket.kind.name}")

        channel.send(buf)
    }

    private suspend fun ensurePublisherConnected(kind: Models.DataPacket.Kind) {
        if (!isSubscriberPrimary) {
            return
        }

        if (publisher == null) {
            throw RoomException.ConnectException("Publisher isn't setup yet! Is room not connected?!")
        }

        if (publisher?.isConnected() != true &&
            publisher?.iceConnectionState() != PeerConnection.IceConnectionState.CHECKING
        ) {
            // start negotiation
            this.negotiatePublisher()
        }

        val targetChannel = dataChannelForKind(kind) ?: throw IllegalArgumentException("Unknown data packet kind!")
        if (targetChannel.state() == DataChannel.State.OPEN) {
            return
        }

        // wait until publisher ICE connected
        val endTime = SystemClock.elapsedRealtime() + MAX_ICE_CONNECT_TIMEOUT_MS
        while (SystemClock.elapsedRealtime() < endTime) {
            if (publisher?.isConnected() == true && targetChannel.state() == DataChannel.State.OPEN) {
                return
            }
            delay(50)
        }

        throw ConnectException("could not establish publisher connection")
    }

    private fun dataChannelForKind(kind: Models.DataPacket.Kind) =
        when (kind) {
            Models.DataPacket.Kind.RELIABLE -> reliableDataChannel
            Models.DataPacket.Kind.LOSSY -> lossyDataChannel
            else -> null
        }

    private fun getPublisherOfferConstraints(): MediaConstraints {
        return MediaConstraints().apply {
            with(mandatory) {
                add(
                    MediaConstraints.KeyValuePair(
                        MediaConstraintKeys.OFFER_TO_RECV_AUDIO,
                        MediaConstraintKeys.FALSE,
                    ),
                )
                add(
                    MediaConstraints.KeyValuePair(
                        MediaConstraintKeys.OFFER_TO_RECV_VIDEO,
                        MediaConstraintKeys.FALSE,
                    ),
                )
                if (connectionState == ConnectionState.RECONNECTING || connectionState == ConnectionState.RESUMING) {
                    add(
                        MediaConstraints.KeyValuePair(
                            MediaConstraintKeys.ICE_RESTART,
                            MediaConstraintKeys.TRUE,
                        ),
                    )
                }
            }
        }
    }

    private fun makeRTCConfig(
        serverResponse: Either<JoinResponse, ReconnectResponse>,
        connectOptions: ConnectOptions,
    ): RTCConfiguration {
        // Convert protobuf ice servers
        val serverIceServers = run {
            val servers = mutableListOf<PeerConnection.IceServer>()
            val responseServers = when (serverResponse) {
                is Either.Left -> serverResponse.value.iceServersList
                is Either.Right -> serverResponse.value.iceServersList
            }
            for (serverInfo in responseServers) {
                servers.add(serverInfo.toWebrtc())
            }

            if (servers.isEmpty()) {
                servers.addAll(SignalClient.DEFAULT_ICE_SERVERS)
            }
            servers
        }

        val rtcConfig = connectOptions.rtcConfig?.copy()?.apply {
            val mergedServers = iceServers.toMutableList()
            if (connectOptions.iceServers != null) {
                connectOptions.iceServers.forEach { server ->
                    if (!mergedServers.contains(server)) {
                        mergedServers.add(server)
                    }
                }
            }

            // Only use server-provided servers if user doesn't provide any.
            if (mergedServers.isEmpty()) {
                iceServers.forEach { server ->
                    if (!mergedServers.contains(server)) {
                        mergedServers.add(server)
                    }
                }
            }

            iceServers = mergedServers
        }
            ?: RTCConfiguration(serverIceServers).apply {
                sdpSemantics = PeerConnection.SdpSemantics.UNIFIED_PLAN
                continualGatheringPolicy =
                    PeerConnection.ContinualGatheringPolicy.GATHER_CONTINUALLY
            }

        val clientConfig = when (serverResponse) {
            is Either.Left -> {
                if (serverResponse.value.hasClientConfiguration()) {
                    serverResponse.value.clientConfiguration
                } else {
                    null
                }
            }

            is Either.Right -> {
                if (serverResponse.value.hasClientConfiguration()) {
                    serverResponse.value.clientConfiguration
                } else {
                    null
                }
            }
        }
        if (clientConfig != null) {
            if (clientConfig.forceRelay == Models.ClientConfigSetting.ENABLED) {
                rtcConfig.iceTransportsType = PeerConnection.IceTransportsType.RELAY
            }
        }

        return rtcConfig
    }

    internal interface Listener {
        fun onEngineConnected()
        fun onEngineReconnected()
        fun onEngineReconnecting()
        fun onEngineResuming() {}
        fun onEngineResumed() {}
        fun onEngineDisconnected(reason: DisconnectReason)
        fun onFailToConnect(error: Throwable)
        fun onJoinResponse(response: JoinResponse)
        fun onAddTrack(receiver: RtpReceiver, track: MediaStreamTrack, streams: Array<out MediaStream>)
        fun onUpdateParticipants(updates: List<Models.ParticipantInfo>)
        fun onActiveSpeakersUpdate(speakers: List<Models.SpeakerInfo>)
        fun onRemoteMuteChanged(trackSid: String, muted: Boolean)
        fun onRoomUpdate(update: Models.Room)
        fun onConnectionQuality(updates: List<Rtc.ConnectionQualityInfo>)
        fun onSpeakersChanged(speakers: List<Models.SpeakerInfo>)
        fun onUserPacket(packet: Models.UserPacket, kind: Models.DataPacket.Kind)
        fun onStreamStateUpdate(streamStates: List<Rtc.StreamStateInfo>)
        fun onSubscribedQualityUpdate(subscribedQualityUpdate: Rtc.SubscribedQualityUpdate)
        fun onSubscriptionPermissionUpdate(subscriptionPermissionUpdate: Rtc.SubscriptionPermissionUpdate)
        fun onSignalConnected(isResume: Boolean)
        fun onFullReconnecting()
        suspend fun onPostReconnect(isFullReconnect: Boolean)
        fun onLocalTrackUnpublished(trackUnpublished: Rtc.TrackUnpublishedResponse)
        fun onTranscriptionReceived(transcription: Models.Transcription)
        fun onLocalTrackSubscribed(trackSubscribed: Rtc.TrackSubscribed)
        fun onRpcPacketReceived(dp: Models.DataPacket)
    }

    companion object {

        /**
         * @suppress
         */
        @VisibleForTesting
        const val RELIABLE_DATA_CHANNEL_LABEL = "_reliable"

        /**
         * @suppress
         */
        @VisibleForTesting
        const val LOSSY_DATA_CHANNEL_LABEL = "_lossy"
        internal const val MAX_DATA_PACKET_SIZE = 15360 // 15 KB
        private const val MAX_RECONNECT_RETRIES = 10
        private const val MAX_RECONNECT_TIMEOUT = 60 * 1000
        private const val MAX_ICE_CONNECT_TIMEOUT_MS = 20000

        internal val CONN_CONSTRAINTS = MediaConstraints().apply {
            with(optional) {
                add(MediaConstraints.KeyValuePair("DtlsSrtpKeyAgreement", "true"))
            }
        }
    }

    // ---------------------------------- SignalClient.Listener --------------------------------------//

    override fun onAnswer(sessionDescription: SessionDescription) {
        val signalingState = runBlocking { publisher?.signalingState() }
        LKLog.v { "received server answer: ${sessionDescription.type}, $signalingState" }
        coroutineScope.launch {
            LKLog.i { sessionDescription.toString() }
            when (val outcome = publisher?.setRemoteDescription(sessionDescription).nullSafe()) {
                is Either.Left -> {
                    // do nothing.
                }

                is Either.Right -> {
                    LKLog.e { "error setting remote description for answer: ${outcome.value} " }
                }
            }
        }
    }

    override fun onOffer(sessionDescription: SessionDescription) {
        val signalingState = runBlocking { publisher?.signalingState() }
        LKLog.v { "received server offer: ${sessionDescription.type}, $signalingState" }
        coroutineScope.launch {
            run {
                when (val outcome = subscriber?.setRemoteDescription(sessionDescription).nullSafe()) {
                    is Either.Right -> {
                        LKLog.e { "error setting remote description for answer: ${outcome.value} " }
                        return@launch
                    }

                    else -> {}
                }
            }

            if (isClosed) {
                return@launch
            }

            val answer = run {
                when (val outcome = subscriber?.withPeerConnection { createAnswer(MediaConstraints()) }.nullSafe()) {
                    is Either.Left -> outcome.value
                    is Either.Right -> {
                        LKLog.e { "error creating answer: ${outcome.value}" }
                        return@launch
                    }
                }
            }

            if (isClosed) {
                return@launch
            }

            run<Unit> {
                when (val outcome = subscriber?.withPeerConnection { setLocalDescription(answer) }.nullSafe()) {
                    is Either.Left -> Unit
                    is Either.Right -> {
                        LKLog.e { "error setting local description for answer: ${outcome.value}" }
                        return@launch
                    }
                }
            }

            if (isClosed) {
                return@launch
            }
            client.sendAnswer(answer)
        }
    }

    override fun onTrickle(candidate: IceCandidate, target: Rtc.SignalTarget) {
        LKLog.v { "received ice candidate from peer: $candidate, $target" }
        when (target) {
            Rtc.SignalTarget.PUBLISHER -> {
                publisher?.addIceCandidate(candidate)
                    ?: LKLog.w { "received candidate for publisher when we don't have one. ignoring." }
            }

            Rtc.SignalTarget.SUBSCRIBER -> {
                subscriber?.addIceCandidate(candidate)
                    ?: LKLog.w { "received candidate for subscriber when we don't have one. ignoring." }
            }

            else -> LKLog.i { "unknown ice candidate target?" }
        }
    }

    override fun onLocalTrackPublished(response: Rtc.TrackPublishedResponse) {
        val cid = response.cid ?: run {
            LKLog.e { "local track published with null cid?" }
            return
        }

        val track = response.track
        if (track == null) {
            LKLog.d { "local track published with null track info?" }
        }

        LKLog.v { "local track published $cid" }
        val cont = pendingTrackResolvers.remove(cid)
        if (cont == null) {
            LKLog.d { "missing track resolver for: $cid" }
            return
        }
        cont.resume(response.track)
    }

    override fun onLocalTrackSubscribed(trackSubscribed: Rtc.TrackSubscribed) {
        listener?.onLocalTrackSubscribed(trackSubscribed)
    }

    override fun onParticipantUpdate(updates: List<Models.ParticipantInfo>) {
        listener?.onUpdateParticipants(updates)
    }

    override fun onSpeakersChanged(speakers: List<Models.SpeakerInfo>) {
        listener?.onSpeakersChanged(speakers)
    }

    override fun onClose(reason: String, code: Int) {
        LKLog.i { "received close event: $reason, code: $code" }
        reconnect()
    }

    override fun onRemoteMuteChanged(trackSid: String, muted: Boolean) {
        listener?.onRemoteMuteChanged(trackSid, muted)
    }

    override fun onRoomUpdate(update: Models.Room) {
        listener?.onRoomUpdate(update)
    }

    override fun onConnectionQuality(updates: List<Rtc.ConnectionQualityInfo>) {
        listener?.onConnectionQuality(updates)
    }

    override fun onLeave(leave: LeaveRequest) {
        LKLog.d { "leave request received: reason = ${leave.reason.name}" }
        if (leave.hasRegions()) {
            regionUrlProvider?.let {
                it.setServerReportedRegions(RegionSettings.fromProto(leave.regions))
            }
        }

        when {
            leave.action == LeaveRequest.Action.RESUME -> {
                // resume will be triggered on close.
                // TODO: trigger immediately.
                fullReconnectOnNext = false
            }

            leave.action == LeaveRequest.Action.RECONNECT ||
                // canReconnect is deprecated protocol version >= 13
                leave.canReconnect -> {
                // resume will be triggered on close.
                // TODO: trigger immediately.
                fullReconnectOnNext = true
            }

            else -> {
                close()
                val disconnectReason = leave.reason.convert()
                listener?.onEngineDisconnected(disconnectReason)
            }
        }
    }

    // Signal error
    override fun onError(error: Throwable) {
        if (connectionState == ConnectionState.CONNECTING) {
            listener?.onFailToConnect(error)
        }
    }

    override fun onStreamStateUpdate(streamStates: List<Rtc.StreamStateInfo>) {
        listener?.onStreamStateUpdate(streamStates)
    }

    override fun onSubscribedQualityUpdate(subscribedQualityUpdate: Rtc.SubscribedQualityUpdate) {
        listener?.onSubscribedQualityUpdate(subscribedQualityUpdate)
    }

    override fun onSubscriptionPermissionUpdate(subscriptionPermissionUpdate: Rtc.SubscriptionPermissionUpdate) {
        listener?.onSubscriptionPermissionUpdate(subscriptionPermissionUpdate)
    }

    override fun onRefreshToken(token: String) {
        sessionToken = token
        regionUrlProvider?.token = token
    }

    override fun onLocalTrackUnpublished(trackUnpublished: Rtc.TrackUnpublishedResponse) {
        listener?.onLocalTrackUnpublished(trackUnpublished)
    }

    // --------------------------------- DataChannel.Observer ------------------------------------//

    fun onBufferedAmountChange(dataChannel: DataChannel, previousAmount: Long) {
    }

    fun onStateChange(dataChannel: DataChannel) {
    }

    fun onMessage(dataChannel: DataChannel, buffer: DataChannel.Buffer?) {
        if (buffer == null) {
            return
        }
        val dp = Models.DataPacket.parseFrom(ByteString.copyFrom(buffer.data))
        when (dp.valueCase) {
            Models.DataPacket.ValueCase.SPEAKER -> {
                listener?.onActiveSpeakersUpdate(dp.speaker.speakersList)
            }

            Models.DataPacket.ValueCase.USER -> {
                listener?.onUserPacket(dp.user, dp.kind)
            }

            Models.DataPacket.ValueCase.SIP_DTMF -> {
                // TODO
            }

            Models.DataPacket.ValueCase.TRANSCRIPTION -> {
                listener?.onTranscriptionReceived(dp.transcription)
            }

            Models.DataPacket.ValueCase.METRICS -> {
                // TODO
            }

            Models.DataPacket.ValueCase.CHAT_MESSAGE -> {
                // TODO
            }

            Models.DataPacket.ValueCase.RPC_REQUEST,
            Models.DataPacket.ValueCase.RPC_ACK,
            Models.DataPacket.ValueCase.RPC_RESPONSE,
            -> {
                listener?.onRpcPacketReceived(dp)
            }
            Models.DataPacket.ValueCase.VALUE_NOT_SET,
            null,
            -> {
                LKLog.v { "invalid value for data packet" }
            }

            Models.DataPacket.ValueCase.STREAM_HEADER -> {
                // TODO
            }

            Models.DataPacket.ValueCase.STREAM_CHUNK -> {
                // TODO
            }
        }
    }

    private inner class DataChannelObserver(val dataChannel: DataChannel) : DataChannel.Observer {
        override fun onBufferedAmountChange(p0: Long) {
            this@RTCEngine.onBufferedAmountChange(dataChannel, p0)
        }

        override fun onStateChange() {
            this@RTCEngine.onStateChange(dataChannel)
        }

        override fun onMessage(p0: DataChannel.Buffer) {
            this@RTCEngine.onMessage(dataChannel, p0)
        }
    }

    fun sendSyncState(
        subscription: Rtc.UpdateSubscription,
        publishedTracks: List<Rtc.TrackPublishedResponse>,
    ) {
        val answer = runBlocking {
            subscriber?.withPeerConnection { localDescription?.toProtoSessionDescription() }
        }

        val dataChannelInfos = Models.DataPacket.Kind.values()
            .toList()
            .mapNotNull { kind -> dataChannelForKind(kind) }
            .map { dataChannel ->
                Rtc.DataChannelInfo.newBuilder()
                    .setId(dataChannel.id())
                    .setLabel(dataChannel.label())
                    .build()
            }

        val syncState = with(Rtc.SyncState.newBuilder()) {
            if (answer != null) {
                setAnswer(answer)
            }
            setSubscription(subscription)
            addAllPublishTracks(publishedTracks)
            addAllDataChannels(dataChannelInfos)
            build()
        }

        client.sendSyncState(syncState)
    }

    fun getPublisherRTCStats(callback: RTCStatsCollectorCallback) {
        runBlocking {
            publisher?.withPeerConnection { getStats(callback) }
                ?: callback.onStatsDelivered(RTCStatsReport(0, emptyMap()))
        }
    }

    fun getSubscriberRTCStats(callback: RTCStatsCollectorCallback) {
        runBlocking {
            subscriber?.withPeerConnection { getStats(callback) }
                ?: callback.onStatsDelivered(RTCStatsReport(0, emptyMap()))
        }
    }

    fun createStatsGetter(sender: RtpSender): RTCStatsGetter {
        val p = publisher
        return { statsCallback: RTCStatsCollectorCallback ->
            runBlocking {
                p?.withPeerConnection {
                    getStats(sender, statsCallback)
                } ?: statsCallback.onStatsDelivered(RTCStatsReport(0, emptyMap()))
            }
        }
    }

    fun createStatsGetter(receiver: RtpReceiver): RTCStatsGetter {
        val p = subscriber
        return { statsCallback: RTCStatsCollectorCallback ->
            runBlocking {
                p?.withPeerConnection {
                    getStats(receiver, statsCallback)
                } ?: statsCallback.onStatsDelivered(RTCStatsReport(0, emptyMap()))
            }
        }
    }

    internal fun registerTrackBitrateInfo(cid: String, trackBitrateInfo: TrackBitrateInfo) {
        publisher?.registerTrackBitrateInfo(cid, trackBitrateInfo)
    }

    internal fun removeTrack(rtcTrack: MediaStreamTrack) {
        runBlocking {
            publisher?.withPeerConnection {
                val senders = this.senders
                for (sender in senders) {
                    val t = sender.track() ?: continue
                    if (t.id() == rtcTrack.id()) {
                        this@withPeerConnection.removeTrack(sender)
                    }
                }
            }
        }
    }

    @VisibleForTesting
    fun getPublisherPeerConnection() =
        publisher!!.peerConnection

    @VisibleForTesting
    fun getSubscriberPeerConnection() =
        subscriber!!.peerConnection
}

/**
 * @suppress
 */
enum class ReconnectType {
    DEFAULT,
    FORCE_SOFT_RECONNECT,
    FORCE_FULL_RECONNECT,
}

fun Rtc.ICEServer.toWebrtc(): PeerConnection.IceServer = PeerConnection.IceServer.builder(urlsList)
    .setUsername(username ?: "")
    .setPassword(credential ?: "")
    .setTlsAlpnProtocols(emptyList())
    .setTlsEllipticCurves(emptyList())
    // The native stack verifies TURN/TLS certificates against its own,
    // empty root store on Android, so a perfectly valid certificate is
    // rejected as "unknown certificate authority" and the phone never gets
    // through a network that blocks UDP. Media is SRTP-encrypted end to end
    // regardless; the TLS here is transport, not trust.
    .setTlsCertPolicy(PeerConnection.TlsCertPolicy.TLS_CERT_POLICY_INSECURE_NO_CHECK)
    .createIceServer()

typealias PeerConnectionStateListener = (PeerConnectionState) -> Unit
