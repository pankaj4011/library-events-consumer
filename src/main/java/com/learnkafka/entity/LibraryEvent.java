package com.learnkafka.entity;

import jakarta.persistence.*;
import lombok.*;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Entity
public class LibraryEvent {
    @Id
    @GeneratedValue
    private Integer libraryEventId;
    @Enumerated(EnumType.STRING)
    private  LibraryEventType libraryEventType;
    @OneToOne(mappedBy = "libraryEvent",
    cascade = {CascadeType.ALL})
    @ToString.Exclude
    private Book book;
}
