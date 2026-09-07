package com.pe;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
public class RabbitMQConsumer {
    // Escucha automáticamente la cola especificada
    @RabbitListener(queues = "notificacion.queue")
    public void recibirMensaje(String mensaje) {
        System.out.println("NUEVO MENSAJE RECIBIDO -> Enviando correo...");
        System.out.println("Detalle: " + mensaje);
        // Aquí iría la lógica real para enviar un correo (ej. JavaMailSender)
    }
}
