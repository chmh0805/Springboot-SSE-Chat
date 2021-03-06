package com.hyuk.chatserver;

import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.hyuk.chatserver.domain.ChatPostDto;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.EmitResult;

@CrossOrigin
@RestController
public class ChatController {

	Sinks.Many<String> sink;

	public ChatController() {
		this.sink = Sinks.many().multicast().onBackpressureBuffer();
	}
	
	@PostMapping("/send")
	public void send(@RequestBody ChatPostDto chatPostDto) {
		EmitResult result = sink.tryEmitNext(chatPostDto.getUsername() + " : " + chatPostDto.getMessage() + "\n");
	}
	
	@GetMapping(value = "/", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
	public Flux<ServerSentEvent<String>> sse() {
		return sink.asFlux().map(e -> ServerSentEvent.builder(e).build()).log().doOnCancel(() -> {
			sink.asFlux().blockLast();
		});
	}
}
