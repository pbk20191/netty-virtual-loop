package io.github.pbk20191.virtualloop.proxy

import java.lang.reflect.Modifier

internal object InterfaceCache : ClassValue<Array<Class<*>>>() {

    override fun computeValue(type: Class<*>): Array<Class<*>> {
        return generateSequence(type) { it.superclass }
            .flatMap {
                var base = it.interfaces.asSequence()
                if (it.isInterface) {
                    base = sequenceOf(it) + base
                }
                base
            }
            .distinct()
            .filter { !it.isHidden && !it.isSealed && Modifier.isPublic(it.modifiers) }
            .toList()
            .toTypedArray()
    }

}