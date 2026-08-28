package io.github.pbk20191.virtualloop.proxy

import java.lang.reflect.Modifier

internal class InterfaceCache: ClassValue<Array<Class<*>>>() {

    override fun computeValue(type: Class<*>): Array<Class<*>> {
        return generateSequence(type) { it.superclass }
            .flatMap { it.interfaces.asSequence() }
            .distinct()
            .filter { !it.isHidden && !it.isSealed && Modifier.isPublic(it.modifiers) }
            .toList()
            .toTypedArray()
    }

}